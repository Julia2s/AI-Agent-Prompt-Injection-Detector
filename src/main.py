from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from mlops.messaging import declare_topology
from src.api.v1.routes import (
    health_router,
    healthz_router,
    tasks_router,
    version_router,
)
from src.api.v1.routes.tasks import store
from src.core.broker import broker, pipeline_settings
from src.core.logging import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(level="DEBUG" if pipeline_settings.debug else "INFO")
    logger.info(
        f"Starting {pipeline_settings.app_name} v{pipeline_settings.app_version}"
    )

    await store.initialize()
    logger.info("Подключение API к RabbitMQ...")
    await broker.connect()
    await declare_topology(broker, pipeline_settings)
    await broker.start()
    logger.info("API успешно подключено к RabbitMQ")

    yield

    logger.info("Остановка API. Отключение от RabbitMQ...")
    await broker.close()
    await store.close()


app = FastAPI(
    title=pipeline_settings.app_name,
    version=pipeline_settings.app_version,
    lifespan=lifespan,
)

app.include_router(healthz_router)
app.include_router(version_router)
app.include_router(health_router)
app.include_router(tasks_router)
