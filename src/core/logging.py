"""Shared stdout logging for pipeline processes."""

import json
import logging
import os
import re
import sys
import traceback
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime

task_id_context: ContextVar[str | None] = ContextVar("task_id", default=None)
DSN_CREDENTIALS = re.compile(r"(\b[a-z][a-z0-9+.-]*://)[^\s/@]+@", re.IGNORECASE)
CREDENTIAL_FIELD = re.compile(
    r"(?i)(\b(?:password|passwd|pwd|api[_-]?key|access[_-]?token|secret)\b[\"']?\s*[=:]\s*)"
    r"(?:'[^']*'|\"[^\"]*\"|[^\s,;&]+)"
)
BEARER_CREDENTIAL = re.compile(r"(?i)(\bBearer\s+)[a-z0-9._~+/-]+={0,2}")
UNSAFE_BROKER_MESSAGES = {
    "message_id if None on returned message %r",
    "Unhandled message %r returning",
}


@contextmanager
def task_context(task_id: str | None):
    token = task_id_context.set(task_id)
    try:
        yield
    finally:
        task_id_context.reset(token)


class JsonFormatter(logging.Formatter):
    def __init__(self, service: str, environment: str):
        super().__init__()
        self.service = service
        self.environment = environment

    def format(self, record: logging.LogRecord) -> str:
        if record.name.startswith(("aiormq.", "aio_pika.")) and record.levelno == logging.DEBUG:
            message = "AMQP library debug event"
        elif (
            record.name.startswith("aiormq.")
            and isinstance(record.msg, str)
            and record.args
            and "frame" in record.msg.lower()
        ):
            message = "AMQP library reported an unexpected frame"
        elif (
            record.name == "aiormq.channel"
            and isinstance(record.msg, str)
            and record.msg in UNSAFE_BROKER_MESSAGES
        ):
            message = "Broker returned a message that could not be matched"
        else:
            message = record.getMessage()
        if record.name == "faststream.broker" and record.exc_info:
            message = "Broker message processing failed"
        message = DSN_CREDENTIALS.sub(r"\1[REDACTED]@", message)
        message = CREDENTIAL_FIELD.sub(r"\1[REDACTED]", message)
        message = BEARER_CREDENTIAL.sub(r"\1[REDACTED]", message)
        entry = {
            "timestamp": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "service": self.service,
            "env": self.environment,
            "logger": record.name,
            "message": message,
        }
        if task_id := task_id_context.get() or getattr(record, "task_id", None):
            entry["task_id"] = task_id
        if hasattr(record, "source_metrics"):
            entry["source_metrics"] = record.source_metrics
        if record.exc_info:
            entry["exception"] = self.safe_exception(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)

    def safe_exception(self, exc_info) -> str:
        exception_type, _, trace = exc_info
        frames = traceback.extract_tb(trace)
        lines = ["Traceback (most recent call last):"]
        lines.extend(
            f'  File "{frame.filename}", line {frame.lineno}, in {frame.name}'
            for frame in frames
        )
        lines.append(exception_type.__name__)
        return "\n".join(lines)


class HealthAccessFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno != logging.INFO:
            return True
        return not any(path in record.getMessage() for path in ("/healthz", "/api/v1/health"))


def setup_logging(service: str) -> None:
    os.environ.setdefault("MLFLOW_LOGGING_CONFIGURE_LOGGING", "false")
    os.environ.setdefault("GIT_PYTHON_REFRESH", "quiet")
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter(service, os.environ.get("ENV", "dev")))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)

    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "faststream", "mlflow"):
        framework_logger = logging.getLogger(name)
        framework_logger.handlers.clear()
        framework_logger.propagate = True
        framework_logger.setLevel(logging.NOTSET)
    access_logger = logging.getLogger("uvicorn.access")
    if not any(isinstance(item, HealthAccessFilter) for item in access_logger.filters):
        access_logger.addFilter(HealthAccessFilter())
