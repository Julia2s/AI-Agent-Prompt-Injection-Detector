"""Stream a CSV one row at a time; never discard a rejected task.

Modes
-----
backpressure (default)  publish -> wait for the broker's confirm -> only then read
                        the next row. A rejected publish (queue full) is retried
                        with async exponential backoff. Memory: one task.
naive                   EXPERIMENT BASELINE ONLY. Fire-and-forget: every row becomes
                        an asyncio task immediately, nothing waits for the broker.
                        This is the anti-pattern the pipeline protects against.
"""

import argparse
import asyncio
import contextlib
import csv
import hashlib
import logging
import time
from pathlib import Path

from aio_pika.exceptions import AMQPException
from opentelemetry import trace

from mlops.messaging import declare_topology, make_broker, new_task, publish_task
from mlops.metrics import SOURCE_COLUMNS, RateSampler, SourceStats, csv_line
from mlops.settings import PipelineSettings
from src.core.logging import setup_logging, task_context
from src.core.tracing import shutdown_tracing

logger = logging.getLogger(__name__)
metrics_logger = logging.getLogger("mlops.producer.metrics")
TRANSIENT_ERRORS = (AMQPException, TimeoutError, ConnectionError, OSError)
tracer = trace.get_tracer(__name__)


async def send_with_backpressure(
    broker, settings, task, sleep=asyncio.sleep, stats: SourceStats | None = None
):
    with task_context(task["task_id"]), tracer.start_as_current_span(
        "source.publish_task"
    ) as span:
        span.set_attribute("app.task_id", task["task_id"])
        await _send_with_backpressure(broker, settings, task, sleep, stats)


async def _send_with_backpressure(broker, settings, task, sleep, stats):
    delay = settings.retry_delay
    while True:
        try:
            await publish_task(broker, settings, task)
            if stats is not None:
                stats.accepted += 1
            logger.info("Task published")
            return
        except TRANSIENT_ERRORS as error:
            if stats is not None:
                stats.rejected += 1
            span = trace.get_current_span()
            span.add_event(
                "publisher.rejected",
                {"app.error.type": type(error).__name__},
            )
            logger.warning(
                "Publication not confirmed; retry in %.2fs",
                delay,
            )
            with tracer.start_as_current_span("source.backoff") as backoff_span:
                backoff_span.set_attribute("app.retry.delay_ms", delay * 1000)
                await sleep(delay)
            delay = min(delay * 2, settings.source_backoff_max)


def rows(path, batch_id, pad_bytes=0):

    with Path(path).open(newline="", encoding="utf-8") as stream:
        for index, row in enumerate(csv.DictReader(stream)):
            text = row["text"]
            key = hashlib.sha256(f"{batch_id}:{index}:{text}".encode()).hexdigest()
            extra = {"padding": "x" * pad_bytes} if pad_bytes else {}
            yield new_task(key, text, **extra)


async def report(sampler, metrics_file, interval=1.0):
    with contextlib.ExitStack() as stack:
        handle = None
        if metrics_file:
            handle = stack.enter_context(Path(metrics_file).open("a", encoding="utf-8"))
            handle.write(",".join(SOURCE_COLUMNS) + "\n")
        while True:
            await asyncio.sleep(interval)
            sample = sampler.sample()
            line = csv_line(sample)
            metrics_logger.info("Source metrics", extra={"source_metrics": sample})
            if handle:
                handle.write(line + "\n")
                handle.flush()


async def run_naive(broker, settings, tasks, stats, deadline):
    pending: set[asyncio.Task] = set()

    def done(future):
        pending.discard(future)
        stats.buffered = len(pending)
        if future.cancelled() or future.exception() is not None:
            stats.lost += 1
        else:
            stats.accepted += 1

    for task in tasks:
        if deadline and time.monotonic() >= deadline:
            break
        future = asyncio.create_task(_publish_naive(broker, settings, task))
        pending.add(future)
        future.add_done_callback(done)
        stats.buffered = len(pending)
        await asyncio.sleep(0)
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def _publish_naive(broker, settings, task):
    with task_context(task["task_id"]), tracer.start_as_current_span(
        "source.publish_task"
    ) as span:
        span.set_attribute("app.task_id", task["task_id"])
        await publish_task(broker, settings, task)


async def run_backpressure(broker, settings, tasks, stats, deadline):
    for task in tasks:
        if deadline and time.monotonic() >= deadline:
            break
        stats.buffered = 1
        await send_with_backpressure(broker, settings, task, stats=stats)
        stats.buffered = 0


async def run(
    path,
    batch_id,
    repeat=1,
    mode="backpressure",
    duration=0.0,
    metrics_file=None,
    pad_bytes=0,
):
    settings = PipelineSettings()
    stats = SourceStats()
    sampler = RateSampler(stats)
    deadline = time.monotonic() + duration if duration else None

    def tasks():
        for cycle in range(repeat):
            yield from rows(path, f"{batch_id}:{cycle}", pad_bytes)

    try:
        async with make_broker(settings, service_name="source") as broker:
            await declare_topology(broker, settings)
            reporter = asyncio.create_task(report(sampler, metrics_file))
            try:
                runner = run_naive if mode == "naive" else run_backpressure
                await runner(broker, settings, tasks(), stats, deadline)
            finally:
                reporter.cancel()
                await asyncio.gather(reporter, return_exceptions=True)
    finally:
        shutdown_tracing()
    logger.info(
        "Finished: accepted=%s rejected_attempts=%s lost=%s",
        stats.accepted,
        stats.rejected,
        stats.lost,
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default="data/processed/train.csv")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument(
        "--mode", choices=["backpressure", "naive"], default="backpressure"
    )
    parser.add_argument(
        "--duration", type=float, default=0.0, help="stop after N seconds"
    )
    parser.add_argument("--metrics-file", default=None)
    parser.add_argument("--pad-bytes", type=int, default=0, help="inflate messages")
    args = parser.parse_args()
    setup_logging("producer")
    asyncio.run(
        run(
            args.file,
            args.batch_id,
            args.repeat,
            args.mode,
            args.duration,
            args.metrics_file,
            args.pad_bytes,
        )
    )
