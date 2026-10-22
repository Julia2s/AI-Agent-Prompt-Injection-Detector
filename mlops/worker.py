"""FastStream worker: bounded concurrency, durable retries, commit before ACK."""

import asyncio
import base64
import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from functools import partial

from faststream import AckPolicy, FastStream
from pydantic import BaseModel, Field, ValidationError

from mlops.db import SUCCESS, ResultStore
from mlops.messaging import dead_queue, declare_topology, make_broker, task_queue
from mlops.settings import PipelineSettings

logger = logging.getLogger(__name__)


class TaskMessage(BaseModel):
    # Unknown fields (e.g. the experiment's `padding`) are ignored by pydantic.
    task_id: str = Field(min_length=1, max_length=128)
    text: str = Field(min_length=1, max_length=100000)
    created_at: float | None = None


async def raw_decoder(message):
    # Bypass FastStream's JSON decoder so malformed payloads reach the handler
    # as raw bytes and go to the DLQ via ValidationError, instead of crashing
    # the consumer on decode.
    return message.body


def load_model(settings):
    import mlflow

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    return mlflow.pyfunc.load_model(settings.model_uri)


class Worker:
    def __init__(self, settings, store=None, model_loader=load_model):
        self.settings = settings
        self.store = store or ResultStore(settings.database_url)
        self.model_loader = model_loader
        self.model = None
        self.broker = make_broker(settings)
        self.pool = ThreadPoolExecutor(max_workers=settings.worker_concurrency)
        # Safety net for the unbounded baseline (prefetch=0): limits the number
        # of concurrent in-flight asyncio tasks inside this process. In bounded
        # mode it is redundant with prefetch_count but harmless.
        self.slots = asyncio.Semaphore(settings.worker_concurrency)
        self.ready = False
        self.handler = self.broker.subscriber(
            task_queue(settings),
            ack_policy=AckPolicy.NACK_ON_ERROR,
            decoder=raw_decoder,
        )(self.process)

    async def start(self):
        await self.store.initialize()
        self.model = await asyncio.get_running_loop().run_in_executor(
            self.pool, self.model_loader, self.settings
        )
        await self.broker.connect()
        await declare_topology(self.broker, self.settings)
        self.ready = True

    def predict(self, text):
        time.sleep(self.settings.inference_delay)
        prediction = self.model.predict([text])
        return bool(prediction[0])

    async def to_dlq(self, body, key, attempts, error):
        await self.broker.publish(
            {
                "message_key": key,
                "body_base64": base64.b64encode(body).decode(),
                "attempts": attempts,
                "error": str(error)[:2000],
            },
            queue=dead_queue(self.settings),
            persist=True,
            mandatory=True,
            timeout=self.settings.publish_timeout,
            message_id=key,
        )

    async def process(self, body: bytes):
        async with self.slots:
            try:
                await self.process_one(body)
            except Exception:
                # DB/DLQ outage: keep the original message, never ACK on failure.
                await asyncio.sleep(self.settings.retry_delay)
                raise

    async def process_one(self, body):
        key = hashlib.sha256(body).hexdigest()
        try:
            task = TaskMessage.model_validate_json(body)
        except ValidationError as error:
            await self.to_dlq(body, key, 0, error)
            return  # Invalid schema is not transient; retry cannot repair it
        existing = await self.store.get(task.task_id)
        if existing is not None:
            if existing.text != task.text:
                await self.to_dlq(body, key, 0, "task_id reused with different text")
                return
            if existing.status == SUCCESS:
                return  # redelivery of an already finished task: nothing to do
            # PENDING row created by the API: fall through and process it.
        attempts = await self.store.failures(key)
        error = "Retry budget was exhausted before redelivery"
        if attempts >= self.settings.max_attempts:
            await self.to_dlq(body, key, attempts, error)
            return
        started = time.perf_counter()
        try:
            prediction = await asyncio.get_running_loop().run_in_executor(
                self.pool, partial(self.predict, task.text)
            )
        except Exception as failure:
            attempts = await self.store.record_failure(key)
            if attempts >= self.settings.max_attempts:
                await self.to_dlq(body, key, attempts, failure)
                return
            # NACK_ON_ERROR requeues the original delivery after process delay.
            raise
        inference_ms = (time.perf_counter() - started) * 1000
        latency_ms = (
            max(0.0, (time.time() - task.created_at) * 1000)
            if task.created_at is not None
            else None
        )
        await self.store.save(
            task.task_id, task.text, prediction, inference_ms, latency_ms
        )
        # Defensive check against a manual publication into the queue with a
        # task_id that already exists with different text (cannot happen via API).
        stored = await self.store.get(task.task_id)
        if stored.text != task.text:
            await self.to_dlq(body, key, attempts, "task_id reused with different text")

    async def health(self, reader, writer):
        await reader.read(4096)
        status = b"200 OK" if self.ready else b"503 Service Unavailable"
        writer.write(
            b"HTTP/1.1 "
            + status
            + b"\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    @asynccontextmanager
    async def lifespan(self):
        server = None
        try:
            await self.start()
            server = await asyncio.start_server(
                self.health, "0.0.0.0", self.settings.worker_health_port
            )
            yield
        finally:
            self.ready = False
            if server:
                server.close()
                await server.wait_closed()
            # FastStream drains accepted deliveries before exiting this lifespan.
            self.pool.shutdown(wait=True)
            await self.store.close()


worker = Worker(PipelineSettings())
app = FastStream(worker.broker, lifespan=worker.lifespan)
