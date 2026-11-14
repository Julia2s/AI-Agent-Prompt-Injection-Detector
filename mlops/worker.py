"""FastStream worker: bounded concurrency, durable retries, commit before ACK."""

import asyncio
import base64
import hashlib
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from contextvars import copy_context
from dataclasses import dataclass
from functools import partial

from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitMessage
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from pydantic import BaseModel, Field, ValidationError

from mlops.db import SUCCESS, ResultStore
from mlops.messaging import dead_queue, declare_topology, make_broker, task_queue
from mlops.settings import PipelineSettings
from src.core.logging import setup_logging, task_context
from src.core.tracing import shutdown_tracing

setup_logging("worker")
logger = logging.getLogger(__name__)
tracer = trace.get_tracer(__name__)


class TaskMessage(BaseModel):
    task_id: str = Field(min_length=1, max_length=128)
    text: str = Field(min_length=1, max_length=100000)
    created_at: float | None = None


async def raw_decoder(message):

    return message.body


@dataclass(frozen=True)
class LoadedModel:
    model: object
    version: str


def load_model_with_metadata(settings):
    import mlflow

    mlflow.set_tracking_uri(settings.mlflow_tracking_uri)
    logger.info(
        "Loading model name=%s alias=%s", settings.model_name, settings.model_alias
    )
    version = (
        mlflow.MlflowClient()
        .get_model_version_by_alias(settings.model_name, settings.model_alias)
        .version
    )
    model = mlflow.pyfunc.load_model(f"models:/{settings.model_name}/{version}")
    logger.info(
        "Model loaded name=%s alias=%s version=%s",
        settings.model_name,
        settings.model_alias,
        version,
    )
    return LoadedModel(model=model, version=str(version))


def load_model(settings):
    """Compatibility helper returning only the model, as in the prior exercise."""
    return load_model_with_metadata(settings).model


class Worker:
    def __init__(self, settings, store=None, model_loader=None):
        self.settings = settings
        self.store = store or ResultStore(settings.database_url)
        self.model_loader = model_loader or load_model_with_metadata
        self.model = None
        self.model_version = "unknown"
        self.broker = make_broker(settings, service_name="worker")
        self.pool = ThreadPoolExecutor(max_workers=settings.worker_concurrency)

        self.slots = asyncio.Semaphore(settings.worker_concurrency)
        self.ready = False
        self.handler = self.broker.subscriber(
            task_queue(settings),
            ack_policy=AckPolicy.NACK_ON_ERROR,
            decoder=raw_decoder,
        )(self.process_message)

    async def process_message(self, body: bytes, message: RabbitMessage):
        with task_context(message.correlation_id), tracer.start_as_current_span(
            "process"
        ) as span:
            span.set_attribute("app.task_id", message.correlation_id or "")
            raw_message = getattr(message, "raw_message", None)
            if raw_message is not None:
                span.set_attribute(
                    "messaging.message.redelivered",
                    bool(getattr(raw_message, "redelivered", False)),
                )
            try:
                await self.process(body)
            except Exception as error:
                span.record_exception(error)
                span.set_status(Status(StatusCode.ERROR))
                raise

    async def start(self):
        await self.store.initialize()
        with tracer.start_as_current_span("model.load") as span:
            span.set_attribute("app.model.alias", self.settings.model_alias)
            context = copy_context()
            loaded = await asyncio.get_running_loop().run_in_executor(
                self.pool,
                context.run,
                partial(self.model_loader, self.settings),
            )
            if isinstance(loaded, LoadedModel):
                self.model = loaded.model
                self.model_version = loaded.version
            else:
                self.model = loaded
                self.model_version = str(
                    getattr(getattr(loaded, "metadata", None), "version", "unknown")
                )
            span.set_attribute("app.model.version", self.model_version)
        await self.broker.connect()
        await declare_topology(self.broker, self.settings)
        self.ready = True

    def predict(self, text):
        time.sleep(self.settings.inference_delay)
        prediction = self.model.predict([text])
        return bool(prediction[0])

    async def to_dlq(self, body, key, attempts, error):
        with tracer.start_as_current_span("dead_letter") as span:
            span.set_attribute(
                "messaging.destination.name", self.settings.queue_name + ".dlq"
            )
            span.set_attribute("app.delivery_attempt", attempts)
            try:
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
            except Exception as failure:
                span.record_exception(failure)
                span.set_status(Status(StatusCode.ERROR))
                raise
            logger.error("Task sent to DLQ after %s attempts", attempts)

    async def process(self, body: bytes):
        async with self.slots:
            try:
                await self.process_one(body)
            except Exception:
                logger.exception("Task processing failed; delivery will be retried")
                span = trace.get_current_span()
                span.add_event(
                    "retry.scheduled",
                    {"app.retry.delay_ms": self.settings.retry_delay * 1000},
                )
                with tracer.start_as_current_span("retry.backoff") as retry_span:
                    retry_span.set_attribute(
                        "app.retry.delay_ms", self.settings.retry_delay * 1000
                    )
                    await asyncio.sleep(self.settings.retry_delay)
                raise

    async def process_one(self, body):
        key = hashlib.sha256(body).hexdigest()
        attempt = await self.store.record_attempt(key)
        process_span = trace.get_current_span()
        process_span.set_attribute("app.delivery_attempt", attempt)
        try:
            task = TaskMessage.model_validate_json(body)
        except ValidationError as error:
            process_span.record_exception(error)
            process_span.set_status(Status(StatusCode.ERROR))
            await self.to_dlq(body, key, attempt, error)
            return
        process_span.set_attribute("app.task_id", task.task_id)
        if task.created_at is not None:
            process_span.set_attribute(
                "app.queue_wait_ms",
                max(0.0, (time.time() - task.created_at) * 1000),
            )
        existing = await self.store.get(task.task_id)
        if existing is not None:
            if existing.text != task.text:
                await self.to_dlq(
                    body, key, attempt, "task_id reused with different text"
                )
                return
            if existing.status == SUCCESS:
                return

        if attempt > self.settings.max_attempts:
            await self.to_dlq(
                body,
                key,
                self.settings.max_attempts,
                "Retry budget was exhausted before redelivery",
            )
            return
        started = time.perf_counter()
        try:
            with tracer.start_as_current_span("inference") as inference_span:
                inference_span.set_attribute(
                    "app.model.alias", self.settings.model_alias
                )
                inference_span.set_attribute("app.model.version", self.model_version)
                inference_span.set_attribute("app.delivery_attempt", attempt)
                context = copy_context()
                prediction = await asyncio.get_running_loop().run_in_executor(
                    self.pool,
                    context.run,
                    partial(self.predict, task.text),
                )
                inference_ms = (time.perf_counter() - started) * 1000
                inference_span.set_attribute(
                    "app.inference.duration_ms", inference_ms
                )
        except Exception as failure:
            process_span.record_exception(failure)
            process_span.set_status(Status(StatusCode.ERROR))
            logger.exception("Inference failed on attempt %s", attempt)
            if attempt >= self.settings.max_attempts:
                await self.to_dlq(
                    body,
                    key,
                    self.settings.max_attempts,
                    failure,
                )
                return

            raise
        latency_ms = (
            max(0.0, (time.time() - task.created_at) * 1000)
            if task.created_at is not None
            else None
        )
        with tracer.start_as_current_span("database.write_result") as db_span:
            db_span.set_attribute("db.system", "postgresql")
            db_span.set_attribute("app.task_id", task.task_id)
            db_span.set_attribute("app.delivery_attempt", attempt)
            try:
                await self.store.save(
                    task.task_id, task.text, prediction, inference_ms, latency_ms
                )
            except Exception as error:
                db_span.record_exception(error)
                db_span.set_status(Status(StatusCode.ERROR))
                raise
        logger.info("Task result saved")

        stored = await self.store.get(task.task_id)
        if stored.text != task.text:
            await self.to_dlq(
                body, key, attempt, "task_id reused with different text"
            )

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
            shutdown_tracing()


worker = Worker(PipelineSettings())
app = FastStream(worker.broker, lifespan=worker.lifespan)
