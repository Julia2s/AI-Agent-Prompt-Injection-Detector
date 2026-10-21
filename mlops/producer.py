"""Stream a CSV one row at a time; never discard a rejected task."""

import argparse
import asyncio
import csv
import hashlib
import logging
from pathlib import Path

from aio_pika.exceptions import AMQPException

from mlops.messaging import declare_topology, make_broker, publish_task
from mlops.settings import PipelineSettings

logger = logging.getLogger(__name__)
TRANSIENT_ERRORS = (AMQPException, TimeoutError, ConnectionError, OSError)


async def send_with_backpressure(broker, settings, task, sleep=asyncio.sleep):
    delay = min(settings.retry_delay, settings.source_backoff_max)
    while True:
        try:
            await publish_task(broker, settings, task)
            return
        except TRANSIENT_ERRORS:
            logger.warning(
                "Publication not confirmed; retry task_id=%s in %.2fs",
                task["task_id"],
                delay,
            )
            await sleep(delay)
            delay = min(delay * 2, settings.source_backoff_max)


def rows(path, batch_id):
    # Stable IDs ==restarting the same input/batch without duplicate results.
    with Path(path).open(newline="", encoding="utf-8") as stream:
        for index, row in enumerate(csv.DictReader(stream)):
            text = row["text"]
            key = hashlib.sha256(f"{batch_id}:{index}:{text}".encode()).hexdigest()
            yield {"task_id": key, "text": text}


async def run(path, batch_id, repeat=1):
    settings = PipelineSettings()
    async with make_broker(settings) as broker:
        await declare_topology(broker, settings)
        count = 0
        for cycle in range(repeat):
            for task in rows(path, f"{batch_id}:{cycle}"):
                await send_with_backpressure(broker, settings, task)
                count += 1
                if count % 100 == 0:
                    logger.info("Confirmed tasks: %s", count)
        logger.info("Finished: %s confirmed tasks", count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", default="data/processed/train.csv")
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run(args.file, args.batch_id, args.repeat))
