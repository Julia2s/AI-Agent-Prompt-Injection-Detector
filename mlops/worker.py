"""FastStream worker: bounded concurrency, durable retries, commit before ACK."""

import asyncio
import base64
import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from contextvars import copy_context
from functools import partial

from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitMessage
from pydantic import BaseModel, Field, ValidationError

from mlops.db import SUCCESS, ResultStore
from mlops.messaging import dead_queue, declare_topology, make_broker, task_queue
from mlops.settings import PipelineSettings
from src.core.logging import setup_logging, task_context

setup_logging("worker")
logger = logging.getLogger(__name__)


class TaskMessage(BaseModel):

    task_id: str = Field(min_length=1, max_length=128)
    text: str = Field(min_length=1, max_length=100000)
    created_at: float | None = None


async def raw_decoder(message):



    return message.body


def load_model(settings):
    import mlflow

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    logger.info(
        "Loading model name=%s alias=%s", settings.model_name, settings.model_alias
    )
    version = mlflow.MlflowClient().get_model_version_by_alias(
        settings.model_name, settings.model_alias
    ).version
    model = mlflow.pyfunc.load_model(f"models:/{settings.model_name}/{version}")
    logger.info(
        "Model loaded name=%s alias=%s version=%s",
        settings.model_name,
        settings.model_alias,
        version,
    )
    return model


class Worker:
    def __init__(self, settings, store=None, model_loader=load_model):
        self.settings = settings
        self.store = store or ResultStore(settings.database_url)
        self.model_loader = model_loader
        self.model = None
        self.broker = make_broker(settings)
        self.pool = ThreadPoolExecutor(max_workers=settings.worker_concurrency)



        self.slots = asyncio.Semaphore(settings.worker_concurrency)
        self.ready = False
        self.handler = self.broker.subscriber(
            task_queue(settings),
            ack_policy=AckPolicy.NACK_ON_ERROR,
            decoder=raw_decoder,
        )(self.process_message)

    async def process_message(self, body: bytes, message: RabbitMessage):
        with task_context(message.correlation_id):
            await self.process(body)

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
        logger.error("Task sent to DLQ after %s attempts", attempts)

    async def process(self, body: bytes):
        async with self.slots:
            try:
                await self.process_one(body)
            except Exception:
                logger.exception("Task processing failed; delivery will be retried")

                await asyncio.sleep(self.settings.retry_delay)
                raise

    async def process_one(self, body):
        key = hashlib.sha256(body).hexdigest()
        try:
            task = TaskMessage.model_validate_json(body)
        except ValidationError as error:
            await self.to_dlq(body, key, 0, error)
            return
        existing = await self.store.get(task.task_id)
        if existing is not None:
            if existing.text != task.text:
                await self.to_dlq(body, key, 0, "task_id reused with different text")
                return
            if existing.status == SUCCESS:
                return

        attempts = await self.store.failures(key)
        error = "Retry budget was exhausted before redelivery"
        if attempts >= self.settings.max_attempts:
            await self.to_dlq(body, key, attempts, error)
            return
        started = time.perf_counter()
        try:
            prediction = await asyncio.get_running_loop().run_in_executor(
                self.pool, copy_context().run, partial(self.predict, task.text)
            )
        except Exception as failure:
            attempts = await self.store.record_failure(key)
            logger.exception("Inference failed on attempt %s", attempts)
            if attempts >= self.settings.max_attempts:
                await self.to_dlq(body, key, attempts, failure)
                return

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
        logger.info("Task result saved")


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
            logger.info("Worker shutdown cleanup started")
            self.ready = False
            if server:
                server.close()
                await server.wait_closed()

            self.pool.shutdown(wait=True)
            await self.store.close()
            logger.info("Worker shutdown cleanup completed")


worker = Worker(PipelineSettings())
app = FastStream(worker.broker, lifespan=worker.lifespan)
