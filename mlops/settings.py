"""Validated pipeline configuration: broker, queue, timings, app metadata."""

import math

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineSettings(BaseSettings):
    app_name: str = "AI Agent Prompt Injection Detector"
    app_version: str = "0.1.0"
    debug: bool = False

    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    database_url: str = "postgresql+asyncpg://user:password@localhost:5432/detector_db"
    mlflow_tracking_uri: str = "http://localhost:5000"
    model_name: str = "prompt-injection-detector"
    model_alias: str = "champion"
    queue_name: str = "inference_queue_v2"
    # 0 means "no limit". Exists ONLY for the baseline experiment in README §7;
    # a production queue must always be bounded.
    queue_capacity: int = Field(default=50, ge=0)
    worker_concurrency: int = Field(default=1, ge=1)
    # 0 means "no limit" (RabbitMQ semantics). Baseline experiment only.
    prefetch_count: int = Field(default=1, ge=0)
    inference_delay: float = Field(default=0.5, ge=0)
    max_attempts: int = Field(default=3, ge=1)
    retry_delay: float = Field(default=0.5, gt=0)
    publish_timeout: float = Field(default=1.0, gt=0)
    source_backoff_max: float = Field(default=2.0, gt=0)
    # How long FastStream waits for already accepted messages on SIGTERM.
    # MUST be a real number: in FastStream 0.7 `None` means "do not wait at all"
    # and running handlers are cancelled. Keep below docker's stop_grace_period.
    graceful_timeout: float = Field(default=300.0, gt=0)
    worker_health_port: int = 8001
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @model_validator(mode="after")
    def check_concurrency(self):
        if self.prefetch_count not in (0, self.worker_concurrency):
            raise ValueError(
                "PREFETCH_COUNT must equal WORKER_CONCURRENCY (or 0 for the "
                "unbounded baseline experiment)"
            )
        return self

    @property
    def bounded(self) -> bool:
        return self.queue_capacity > 0

    @property
    def model_uri(self):
        return f"models:/{self.model_name}@{self.model_alias}"

    @property
    def retry_after(self):
        # Time for one worker slot to be released (inference + retry pause),
        # rounded up: Retry-After is whole seconds.
        return max(1, math.ceil(self.inference_delay + self.retry_delay))
