"""Subprocess helper for the real SIGTERM test; never used by production."""

import asyncio
import os
import time
from pathlib import Path

from faststream import FastStream

from mlops.settings import PipelineSettings
from mlops.worker import Worker


class SlowModel:
    def predict(self, values):
        Path(os.environ["MARKER"]).touch()
        time.sleep(1.5)
        return [1]


if __name__ == "__main__":
    worker = Worker(
        PipelineSettings(worker_health_port=0), model_loader=lambda _: SlowModel()
    )
    asyncio.run(FastStream(worker.broker, lifespan=worker.lifespan).run())
