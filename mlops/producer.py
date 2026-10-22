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

from mlops.messaging import declare_topology, make_broker, new_task, publish_task
from mlops.metrics import SOURCE_COLUMNS, RateSampler, SourceStats, csv_line
from mlops.settings import PipelineSettings

logger = logging.getLogger(__name__)
TRANSIENT_ERRORS = (AMQPException, TimeoutError, ConnectionError, OSError)
METRIC_PREFIX = "SOURCE_METRIC,"


async def send_with_backpressure(
    broker, settings, task, sleep=asyncio.sleep, stats: SourceStats | None = None
):
    delay = settings.retry_delay
    while True:
        try:
            await publish_task(broker, settings, task)
            if stats is not None:
                stats.accepted += 1
            return
        except TRANSIENT_ERRORS:
            if stats is not None:
                stats.rejected += 1
            logger.warning(
                "Publication not confirmed; retry task_id=%s in %.2fs",
                task["task_id"],
                delay,
            )
            await sleep(delay)
            delay = min(delay * 2, settings.source_backoff_max)


def rows(path, batch_id, pad_bytes=0):
    # Stable IDs: restarting the same input/batch gives no duplicate results.
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
            line = csv_line(sampler.sample())
            print(METRIC_PREFIX + line, flush=True)
            if handle:
                handle.write(line + "\n")
                handle.flush()


async def run_naive(broker, settings, tasks, stats, deadline):
    pending: set[asyncio.Task] = set()

    def done(future):
        pending.discard(future)
        stats.buffered = len(pending)
        if future.cancelled() or future.exception() is not None:
            stats.lost += 1  # nobody retries: the task is gone
        else:
            stats.accepted += 1

    for task in tasks:
        if deadline and time.monotonic() >= deadline:
            break
        future = asyncio.create_task(publish_task(broker, settings, task))
        pending.add(future)
        future.add_done_callback(done)
        stats.buffered = len(pending)
        await asyncio.sleep(0)  # let the loop breathe, never wait for the broker
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def run_backpressure(broker, settings, tasks, stats, deadline):
    for task in tasks:
        if deadline and time.monotonic() >= deadline:
            break
        stats.buffered = 1  # exactly one task is held while we wait for the broker
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

    async with make_broker(settings) as broker:
        await declare_topology(broker, settings)
        reporter = asyncio.create_task(report(sampler, metrics_file))
        try:
            runner = run_naive if mode == "naive" else run_backpressure
            await runner(broker, settings, tasks(), stats, deadline)
        finally:
            reporter.cancel()
            await asyncio.gather(reporter, return_exceptions=True)
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
    logging.basicConfig(level=logging.INFO)
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
