"""Shared, validated pipeline configuration."""

import math

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class PipelineSettings(BaseSettings):
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    database_url: str = "postgresql+asyncpg://user:password@localhost:5432/detector_db"
    mlflow_tracking_uri: str = "http://localhost:5000"
    model_name: str = "prompt-injection-detector"
    model_alias: str = "champion"
    queue_name: str = "inference_queue_v2"
    queue_capacity: int = Field(default=50, ge=1)
    worker_concurrency: int = Field(default=1, ge=1)
    prefetch_count: int = Field(default=1, ge=1)
    inference_delay: float = Field(default=0.5, ge=0)
    max_attempts: int = Field(default=3, ge=1)
    retry_delay: float = Field(default=0.5, gt=0)
    publish_timeout: float = Field(default=1.0, gt=0)
    source_backoff_max: float = Field(default=2.0, gt=0)
    worker_health_port: int = 8001
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @model_validator(mode="after")
    def check_concurrency(self):
        if self.prefetch_count != self.worker_concurrency:
            raise ValueError("PREFETCH_COUNT must equal WORKER_CONCURRENCY")
        return self

    @property
    def model_uri(self):
        return f"models:/{self.model_name}@{self.model_alias}"

    @property
    def retry_after(self):
        return max(1, math.ceil(self.inference_delay + self.retry_delay))
