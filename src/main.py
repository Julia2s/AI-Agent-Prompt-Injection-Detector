from contextlib import asynccontextmanager
from fastapi import FastAPI
from loguru import logger

from src.api.v1.routes import (
    health_router,
    healthz_router,
    version_router,
    tasks_router,
)
from src.core.config import settings
from src.core.logging import setup_logging
from src.core.broker import broker

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging(level="DEBUG" if settings.DEBUG else "INFO")
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    
    logger.info("Подключение API к RabbitMQ...")
    await broker.start()
    logger.info("API успешно подключено к RabbitMQ")
    
    yield 
    
    logger.info("Остановка API. Отключение от RabbitMQ...")
    await broker.close()

app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
)

app.include_router(healthz_router)
app.include_router(version_router)
app.include_router(health_router)
app.include_router(tasks_router)