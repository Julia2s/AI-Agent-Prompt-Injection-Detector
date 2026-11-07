import asyncio
import json
import logging
import sys
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from faststream.rabbit import TestRabbitBroker as RabbitTestBroker

from mlops.messaging import dead_queue, make_broker, task_queue
from mlops.producer import send_with_backpressure
from mlops.settings import PipelineSettings
from mlops.worker import Worker, load_model
from src.core.logging import JsonFormatter, task_id_context


@pytest.fixture
async def worker(tmp_path):
    settings = PipelineSettings(
        database_url=f"sqlite+aiosqlite:///{tmp_path}/test.db",
        inference_delay=0.01,
        retry_delay=0.001,
    )
    w = Worker(settings)
    await w.store.initialize()
    w.model = Mock()
    w.model.predict.return_value = [1]
    yield w
    w.pool.shutdown(wait=True)
    await w.store.close()


@pytest.mark.asyncio
async def test_handler_commits_once_on_duplicate(worker):
    async with RabbitTestBroker(worker.broker):
        task = {"task_id": "same", "text": "hello"}
        await worker.broker.publish(task, queue=task_queue(worker.settings))
        await worker.broker.publish(task, queue=task_queue(worker.settings))
    assert (await worker.store.get("same")).is_injection is True
    assert worker.model.predict.call_count == 1


@pytest.mark.asyncio
async def test_handler_receives_correlation_context_and_resets(worker):
    seen = []
    worker.process_one = AsyncMock(side_effect=lambda body: seen.append(task_id_context.get()))
    async with RabbitTestBroker(worker.broker):
        await worker.broker.publish(
            {"task_id": "t1", "text": "hi"},
            queue=task_queue(worker.settings),
            correlation_id="t1",
        )
    assert seen == ["t1"]
    assert task_id_context.get() is None


@pytest.mark.asyncio
async def test_inference_thread_receives_task_context(worker):
    seen = []
    worker.model.predict.side_effect = lambda texts: (seen.append(task_id_context.get()) or [1])
    async with RabbitTestBroker(worker.broker):
        await worker.broker.publish(
            {"task_id": "thread-task", "text": "hi"},
            queue=task_queue(worker.settings),
            correlation_id="thread-task",
        )
    assert seen == ["thread-task"]
    assert task_id_context.get() is None


@pytest.mark.asyncio
async def test_atomic_concurrent_result_insert(worker):
    await asyncio.gather(*(worker.store.save("same", "hello", True) for _ in range(8)))
    from sqlalchemy import func, select

    from mlops.db import TaskResult

    async with worker.store.sessions() as session:
        assert await session.scalar(select(func.count()).select_from(TaskResult)) == 1


@pytest.mark.asyncio
async def test_poison_budget_survives_redelivery(worker):
    worker.model.predict.side_effect = ValueError("bad model input")
    received = []

    @worker.broker.subscriber(dead_queue(worker.settings))
    async def collect(message: dict):
        received.append(message)

    async with RabbitTestBroker(worker.broker):
        for _ in range(2):
            with pytest.raises(ValueError, match="bad model input"):
                await worker.broker.publish(
                    {"task_id": "bad", "text": "bad"}, queue=task_queue(worker.settings)
                )
        for _ in range(2):
            await worker.broker.publish(
                {"task_id": "bad", "text": "bad"}, queue=task_queue(worker.settings)
            )
    assert worker.model.predict.call_count == 3
    assert len(received) == 2
    assert all(m["attempts"] == 3 for m in received)
    assert await worker.store.get("bad") is None


@pytest.mark.asyncio
async def test_malformed_json_goes_to_dlq(worker):
    received = []

    @worker.broker.subscriber(dead_queue(worker.settings))
    async def collect(message: dict):
        received.append(message)

    async with RabbitTestBroker(worker.broker):
        await worker.broker.publish(b"not JSON", queue=task_queue(worker.settings))
    assert len(received) == 1
    worker.model.predict.assert_not_called()


@pytest.mark.asyncio
async def test_dlq_log_has_context_without_body(worker):
    entries = []

    @worker.broker.subscriber(dead_queue(worker.settings))
    async def receive_dlq(message: dict):
        return None

    class Capture(logging.Handler):
        def emit(self, record):
            entries.append(json.loads(JsonFormatter("worker", "test").format(record)))

    log = logging.getLogger("mlops.worker")
    handler = Capture()
    log.addHandler(handler)
    try:
        async with RabbitTestBroker(worker.broker):
            await worker.broker.publish(
                b"private malformed payload",
                queue=task_queue(worker.settings),
                correlation_id="poison-id",
            )
    finally:
        log.removeHandler(handler)
    event = next(entry for entry in entries if "sent to DLQ" in entry["message"])
    assert event["level"] == "ERROR"
    assert event["task_id"] == "poison-id"
    assert "private malformed payload" not in json.dumps(entries)


@pytest.mark.asyncio
async def test_temporary_inference_failure_recovers(worker):
    worker.model.predict.side_effect = [ValueError("temporary"), [0]]
    async with RabbitTestBroker(worker.broker):
        with pytest.raises(ValueError, match="temporary"):
            await worker.broker.publish(
                {"task_id": "recover", "text": "hi"}, queue=task_queue(worker.settings)
            )
        await worker.broker.publish(
            {"task_id": "recover", "text": "hi"}, queue=task_queue(worker.settings)
        )
    assert (await worker.store.get("recover")).is_injection is False
    assert worker.model.predict.call_count == 2


@pytest.mark.asyncio
async def test_failure_log_has_attempt_without_payload(worker, monkeypatch):
    failure_log = Mock()
    monkeypatch.setattr("mlops.worker.logger.exception", failure_log)
    worker.model.predict.side_effect = ValueError("sensitive text")
    async with RabbitTestBroker(worker.broker):
        with pytest.raises(ValueError):
            await worker.broker.publish(
                {"task_id": "id", "text": "sensitive text"},
                queue=task_queue(worker.settings),
                correlation_id="id",
            )
    failure_log.assert_any_call("Inference failed on attempt %s", 1)


@pytest.mark.asyncio
async def test_store_error_is_not_swallowed(worker):
    worker.store.save = AsyncMock(side_effect=ConnectionError("DB offline"))
    with pytest.raises(ConnectionError):
        await worker.process(json.dumps({"task_id": "x", "text": "hi"}).encode())


@pytest.mark.asyncio
async def test_dlq_failure_is_not_swallowed(worker):
    worker.broker.publish = AsyncMock(side_effect=ConnectionError("DLQ offline"))
    with pytest.raises(ConnectionError):
        await worker.process(b"invalid")


@pytest.mark.asyncio
async def test_event_loop_and_health_remain_responsive(worker):
    worker.settings.inference_delay = 0.2
    pending = asyncio.create_task(worker.process(b'{"task_id":"x","text":"hi"}'))
    worker.ready = True
    server = await asyncio.start_server(worker.health, "127.0.0.1", 0)
    try:
        await asyncio.sleep(0.02)
        start = time.monotonic()
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", server.sockets[0].getsockname()[1]
        )
        writer.write(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await writer.drain()
        assert b"200 OK" in await reader.read()
        assert time.monotonic() - start < 0.15
        assert not pending.done()
        writer.close()
        await writer.wait_closed()
        await pending
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_source_retries_same_task_with_backoff():
    broker = Mock()
    broker.publish = AsyncMock(side_effect=[TimeoutError(), ConnectionError(), None])
    sleep = AsyncMock()
    task = {"task_id": "keep-me", "text": "hi"}
    await send_with_backpressure(broker, PipelineSettings(), task, sleep=sleep)
    assert broker.publish.await_count == 3
    assert all(call.args[0] is task for call in broker.publish.await_args_list)
    assert [call.args[0] for call in sleep.await_args_list] == [0.5, 1.0]


@pytest.mark.asyncio
async def test_model_loads_once_at_start(worker):
    worker.model_loader = Mock(return_value=worker.model)
    async with RabbitTestBroker(worker.broker):
        await worker.start()
        for i in range(2):
            await worker.broker.publish(
                {"task_id": str(i), "text": "hi"}, queue=task_queue(worker.settings)
            )
    worker.model_loader.assert_called_once_with(worker.settings)
    assert worker.settings.model_uri == "models:/prompt-injection-detector@champion"


def test_model_alias_resolves_to_version_before_loading(monkeypatch):
    settings = PipelineSettings(model_name="detector", model_alias="champion")
    client = Mock()
    client.get_model_version_by_alias.return_value.version = "42"
    model = object()
    load = Mock(return_value=model)
    info = Mock()
    mlflow = SimpleNamespace(
        set_tracking_uri=Mock(),
        MlflowClient=Mock(return_value=client),
        pyfunc=SimpleNamespace(load_model=load),
    )
    monkeypatch.setitem(sys.modules, "mlflow", mlflow)
    monkeypatch.setattr("mlops.worker.logger.info", info)

    assert load_model(settings) is model
    client.get_model_version_by_alias.assert_called_once_with("detector", "champion")
    load.assert_called_once_with("models:/detector/42")
    info.assert_any_call(
        "Model loaded name=%s alias=%s version=%s", "detector", "champion", "42"
    )


def test_prefetch_matches_pool():
    with pytest.raises(ValueError, match="PREFETCH_COUNT"):
        PipelineSettings(prefetch_count=2, worker_concurrency=1)


def test_unbounded_queue_is_baseline_only():
    bounded = task_queue(PipelineSettings())
    assert bounded.arguments["x-max-length"] == 50
    assert bounded.arguments["x-overflow"] == "reject-publish"
    baseline = task_queue(PipelineSettings(queue_capacity=0, prefetch_count=0))
    assert "x-max-length" not in baseline.arguments
    assert "x-overflow" not in baseline.arguments


def test_graceful_timeout_is_a_real_number():
    broker = make_broker(PipelineSettings(graceful_timeout=42))
    assert broker.config.graceful_timeout == 42
    with pytest.raises(ValueError):
        PipelineSettings(graceful_timeout=0)


@pytest.mark.asyncio
async def test_shutdown_logs_wrap_broker_stop(monkeypatch):
    from faststream import FastStream

    events = []
    broker = make_broker(PipelineSettings())
    app = FastStream(broker)
    monkeypatch.setattr(app, "_log", lambda level, message: events.append(message))

    async def stop():
        events.append("broker stopped")

    monkeypatch.setattr(broker, "stop", stop)
    await app._shutdown()
    assert events == [
        "FastStream app shutting down...",
        "broker stopped",
        "FastStream app shut down gracefully.",
    ]


@pytest.mark.asyncio
async def test_pending_row_is_completed_not_skipped(worker):
    await worker.store.create_pending("p1", "hello")
    async with RabbitTestBroker(worker.broker):
        await worker.broker.publish(
            {"task_id": "p1", "text": "hello", "created_at": time.time() - 2},
            queue=task_queue(worker.settings),
        )
    row = await worker.store.get("p1")
    assert row.status == "SUCCESS" and row.is_injection is True
    assert row.inference_ms >= worker.settings.inference_delay * 1000
    assert row.latency_ms >= 2000
    assert row.finished_at is not None


@pytest.mark.asyncio
async def test_finished_result_is_never_overwritten(worker):
    await worker.store.save("done", "hello", True, 1.0, 1.0)
    await worker.store.save("done", "hello", False, 9.0, 9.0)
    row = await worker.store.get("done")
    assert row.is_injection is True and row.inference_ms == 1.0


@pytest.mark.asyncio
async def test_pending_text_mismatch_goes_to_dlq(worker):
    await worker.store.create_pending("m1", "original")
    received = []

    @worker.broker.subscriber(dead_queue(worker.settings))
    async def collect(message: dict):
        received.append(message)

    async with RabbitTestBroker(worker.broker):
        await worker.broker.publish(
            {"task_id": "m1", "text": "different"}, queue=task_queue(worker.settings)
        )
    assert len(received) == 1
    worker.model.predict.assert_not_called()
