import asyncio
import json
import logging
from unittest.mock import AsyncMock, Mock

import pytest

from mlops.messaging import BrokerLogger, make_broker, publish_task
from mlops.settings import PipelineSettings
from src.core.logging import JsonFormatter, setup_logging, task_context, task_id_context


def test_json_formatter_includes_shared_fields_and_exception():
    try:
        raise ValueError("example failure")
    except ValueError:
        import sys

        record = logging.LogRecord(
            "library", logging.ERROR, __file__, 1, "failed %s", ("task",),
            sys.exc_info(),
        )
    entry = json.loads(JsonFormatter("worker", "test").format(record))
    assert entry["service"] == "worker"
    assert entry["env"] == "test"
    assert entry["level"] == "ERROR"
    assert entry["logger"] == "library"
    assert entry["message"] == "failed task"
    assert "ValueError" in entry["exception"]
    assert "example failure" not in entry["exception"]
    assert "test_json_formatter_includes_shared_fields_and_exception" in entry["exception"]
    assert entry["timestamp"].endswith("+00:00")


def test_faststream_exception_and_dsn_are_not_exposed():
    try:
        raise ValueError("secret task content")
    except ValueError:
        import sys

        record = logging.LogRecord(
            "faststream.broker", logging.ERROR, "", 1,
            "ValueError: secret task content", (), sys.exc_info(),
        )
    formatter = JsonFormatter("worker", "test")
    output = formatter.format(record)
    assert "secret task content" not in output
    assert "Broker message processing failed" in output
    record = logging.LogRecord(
        "library", logging.WARNING, "", 1,
        "connection postgresql+asyncpg://user:password@db:5432/name", (), None,
    )
    output = formatter.format(record)
    assert "user:password" not in output
    assert "[REDACTED]@db" in output


def test_broker_message_body_and_connection_secrets_are_not_exposed():
    formatter = JsonFormatter("producer", "test")
    record = logging.LogRecord(
        "aiormq.channel", logging.ERROR, "", 1,
        "Unhandled message %r returning", (b"sensitive task text",), None,
    )
    output = formatter.format(record)
    assert "sensitive task text" not in output
    assert "Broker returned a message" in output
    record = logging.LogRecord(
        "library", logging.WARNING, "", 1,
        "connection password='my secret' api_key=secret123 bearer token123", (), None,
    )
    output = formatter.format(record)
    assert "my secret" not in output
    assert "secret123" not in output
    assert "token123" not in output


def test_amqp_debug_frames_do_not_expose_payload():
    formatter = JsonFormatter("producer", "test")
    for name, level, template in (
        ("aiormq.connection", logging.DEBUG, "Prepare to send %r"),
        ("aio_pika.exchange", logging.DEBUG, "Publishing %r"),
        ("aiormq.connection", logging.ERROR, "Unexpected frame %r"),
    ):
        record = logging.LogRecord(name, level, "", 1, template, (b"private payload",), None)
        assert "private payload" not in formatter.format(record)


def test_setup_uses_environment_and_routes_library_logs_to_stdout(monkeypatch, capsys):
    root = logging.getLogger()
    frameworks = [logging.getLogger(name) for name in (
        "uvicorn", "uvicorn.error", "uvicorn.access", "faststream"
    )]
    saved_root = (root.handlers[:], root.level)
    saved_frameworks = [(log, log.handlers[:], log.level, log.propagate) for log in frameworks]
    monkeypatch.setenv("LOG_LEVEL", "WARNING")
    monkeypatch.setenv("ENV", "test")
    try:
        setup_logging("api")
        logging.getLogger("uvicorn.error").info("hidden")
        logging.getLogger("uvicorn.error").warning("visible")
        entry = json.loads(capsys.readouterr().out)
        assert entry["message"] == "visible"
        assert (entry["service"], entry["env"], entry["level"]) == (
            "api", "test", "WARNING"
        )
    finally:
        root.handlers[:], root.level = saved_root
        for log, handlers, level, propagate in saved_frameworks:
            log.handlers[:], log.level, log.propagate = handlers, level, propagate


@pytest.mark.asyncio
async def test_context_is_isolated_and_reset():
    async def capture(task_id):
        with task_context(task_id):
            await asyncio.sleep(0)
            record = logging.LogRecord("test", logging.INFO, "", 1, "event", (), None)
            return json.loads(JsonFormatter("worker", "test").format(record))["task_id"]

    assert await asyncio.gather(capture("one"), capture("two")) == ["one", "two"]
    assert task_id_context.get() is None


@pytest.mark.asyncio
async def test_publish_uses_task_id_as_correlation_id():
    broker = AsyncMock()
    await publish_task(broker, PipelineSettings(), {"task_id": "abc", "text": "x"})
    assert broker.publish.await_args.kwargs["correlation_id"] == "abc"


def test_broker_uses_shared_logger(monkeypatch):
    broker_factory = Mock()
    monkeypatch.setattr("mlops.messaging.RabbitBroker", broker_factory)
    make_broker(PipelineSettings())
    broker_logger = broker_factory.call_args.kwargs["logger"]
    assert isinstance(broker_logger, BrokerLogger)
    assert broker_logger.logger is logging.getLogger("faststream.broker")
    assert broker_logger.broker is broker_factory.return_value


def test_broker_logger_uses_message_correlation_id():
    adapter = BrokerLogger()
    adapter.broker = Mock()
    adapter.broker.context.get_local.return_value.correlation_id = "full-task-id"
    _, kwargs = adapter.process("Received", {"extra": {"queue": "work"}})
    assert kwargs["extra"] == {"queue": "work", "task_id": "full-task-id"}
