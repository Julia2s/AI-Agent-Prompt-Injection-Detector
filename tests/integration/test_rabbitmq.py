"""Real broker tests. Run explicitly: pytest -m integration tests/integration -v."""

import asyncio
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from unittest.mock import Mock

import aio_pika
import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from testcontainers.community.postgres import PostgresContainer
from testcontainers.community.rabbitmq import RabbitMqContainer

from mlops.db import ResultStore
from mlops.messaging import declare_topology, make_broker, publish_task
from mlops.producer import TRANSIENT_ERRORS, send_with_backpressure
from mlops.settings import PipelineSettings
from mlops.worker import Worker
from src.api.v1.routes import tasks

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def services():
    # Intentionally fail if Docker is unavailable: never report this as a passing test.
    with (
        RabbitMqContainer("rabbitmq:3.13-management") as rabbit,
        PostgresContainer("postgres:15-alpine") as postgres,
    ):
        yield (
            f"amqp://guest:guest@{rabbit.get_container_host_ip()}:{rabbit.get_exposed_port(5672)}/",
            postgres.get_connection_url().replace(
                "postgresql+psycopg2", "postgresql+asyncpg"
            ),
        )


@pytest.fixture
async def settings(services):
    return PipelineSettings(
        rabbitmq_url=services[0],
        database_url=services[1],
        queue_name="test_" + uuid.uuid4().hex,
        queue_capacity=2,
        retry_delay=0.05,
        inference_delay=0.05,
        publish_timeout=1,
    )


async def wait_for(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = await predicate()
        if value:
            return value
        await asyncio.sleep(0.05)
    raise AssertionError("Condition was not met before deadline")


@pytest.mark.asyncio
async def test_real_overflow_backpressure_and_api(settings, monkeypatch):
    async with make_broker(settings) as broker:
        await declare_topology(broker, settings)
        for index in range(settings.queue_capacity):
            await publish_task(broker, settings, {"task_id": str(index), "text": "hi"})
        with pytest.raises(TRANSIENT_ERRORS):
            await publish_task(broker, settings, {"task_id": "rejected", "text": "hi"})
        store = ResultStore(settings.database_url)
        await store.initialize()
        monkeypatch.setattr(tasks, "broker", broker)
        monkeypatch.setattr(tasks, "pipeline_settings", settings)
        monkeypatch.setattr(tasks, "store", store)
        app = FastAPI()
        app.include_router(tasks.router)
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            started = time.monotonic()
            response = await client.post(
                "/api/v1/tasks", json={"task_id": "api-full", "text": "hi"}
            )
        assert response.status_code == 503
        assert "Retry-After" in response.headers
        assert time.monotonic() - started < settings.publish_timeout + 0.5
        assert await store.get("api-full") is None  # no PENDING ghost left behind
        assert response.json()["detail"]["task_id"] == "api-full"
        pending = asyncio.create_task(
            send_with_backpressure(
                broker, settings, {"task_id": "retained", "text": "hi"}
            )
        )
        try:
            await asyncio.sleep(0.2)
            assert not pending.done()
            connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(settings.queue_name, passive=True)
                assert queue.declaration_result.message_count == settings.queue_capacity
                message = await queue.get(timeout=2)
                await message.ack()
                await asyncio.wait_for(pending, 5)
                remaining = [
                    await queue.get(timeout=2) for _ in range(settings.queue_capacity)
                ]
                assert any(
                    json.loads(m.body)["task_id"] == "retained" for m in remaining
                )
                for message in remaining:
                    await message.ack()
        finally:
            if not pending.done():
                pending.cancel()
            await store.close()


@pytest.mark.asyncio
async def test_real_worker_duplicate_and_dlq(settings):
    model = Mock()
    model.predict.side_effect = lambda values: (
        (_ for _ in ()).throw(ValueError("poison")) if values[0] == "poison" else [1]
    )
    worker = Worker(settings, model_loader=lambda _: model)
    await worker.start()
    await worker.broker.start()
    try:
        async with make_broker(settings) as publisher:
            task = {"task_id": uuid.uuid4().hex, "text": "hi"}
            await send_with_backpressure(publisher, settings, task)
            await send_with_backpressure(publisher, settings, task)
            await wait_for(lambda: worker.store.get(task["task_id"]))
            bad = {"task_id": uuid.uuid4().hex, "text": "poison"}
            await send_with_backpressure(publisher, settings, bad)
            connection = await aio_pika.connect_robust(settings.rabbitmq_url)
            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(
                    settings.queue_name + ".dlq", passive=True
                )

                async def dead():
                    return await queue.get(fail=False)

                message = await wait_for(dead)
                assert json.loads(message.body)["attempts"] == settings.max_attempts
                await message.ack()
            assert await worker.store.get(bad["task_id"]) is None
            assert model.predict.call_count == 4
    finally:
        await worker.broker.stop()
        worker.pool.shutdown(wait=True)
        await worker.store.close()


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows has no real SIGTERM: send_signal() kills the process at once. "
    "Run this test on Linux/WSL/CI, or check graceful shutdown in docker compose.",
)
@pytest.mark.asyncio
async def test_sigterm_finishes_accepted_message(settings, tmp_path):
    marker = tmp_path / "inference-started"
    env = dict(
        os.environ,
        RABBITMQ_URL=settings.rabbitmq_url,
        DATABASE_URL=settings.database_url,
        QUEUE_NAME=settings.queue_name,
        QUEUE_CAPACITY=str(settings.queue_capacity),
        MARKER=str(marker),
    )
    # Only model loading is replaced: real process, FastStream, broker and database.
    process = subprocess.Popen(
        [sys.executable, "-m", "tests.integration.shutdown_worker"], env=env
    )
    store = ResultStore(settings.database_url)
    await store.initialize()
    task_id = uuid.uuid4().hex
    try:
        async with make_broker(settings) as publisher:
            await declare_topology(publisher, settings)
            await publish_task(publisher, settings, {"task_id": task_id, "text": "hi"})
        deadline = time.monotonic() + 20
        while not marker.exists() and time.monotonic() < deadline:
            assert process.poll() is None
            await asyncio.sleep(0.05)
        assert marker.exists(), "Worker never accepted the message"
        process.send_signal(signal.SIGTERM)
        await asyncio.wait_for(asyncio.to_thread(process.wait), 15)
        assert process.returncode == 0
        assert await store.get(task_id) is not None
        connection = await aio_pika.connect_robust(settings.rabbitmq_url)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue(settings.queue_name, passive=True)
            assert await queue.get(fail=False) is None
    finally:
        if process.poll() is None:
            process.kill()
            await asyncio.to_thread(process.wait)
        await store.close()


@pytest.mark.asyncio
async def test_source_rate_converges_to_worker_throughput(settings):
    """Steady state: accepted publish rate == worker throughput; depth <= limit."""
    settings.inference_delay = 0.1  # one thread -> ~10 tasks/s
    settings.queue_capacity = 5
    settings.queue_name = "rate_" + uuid.uuid4().hex
    model = Mock()
    model.predict.return_value = [1]
    worker = Worker(settings, model_loader=lambda _: model)
    await worker.start()
    await worker.broker.start()
    depth, accepted_at = [], []
    connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    try:
        async with connection, make_broker(settings) as publisher:
            channel = await connection.channel()
            await channel.declare_queue(settings.queue_name, passive=True)

            async def source():
                for index in range(10_000):
                    await send_with_backpressure(
                        publisher, settings, {"task_id": f"r{index}", "text": "hi"}
                    )
                    accepted_at.append(time.monotonic())

            task = asyncio.create_task(source())
            started = time.monotonic()
            while time.monotonic() - started < 8:
                await asyncio.sleep(0.1)
                declared = await channel.declare_queue(
                    settings.queue_name, passive=True
                )
                depth.append(declared.declaration_result.message_count)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    finally:
        await worker.broker.stop()
        worker.pool.shutdown(wait=True)
        await worker.store.close()
    assert max(depth) <= settings.queue_capacity
    steady = [t for t in accepted_at if t - started > 3]  # skip the fill-up phase
    rate = len(steady) / (accepted_at[-1] - (started + 3))
    assert 5 <= rate <= 15, f"source rate {rate:.1f}/s vs worker ~10/s"
