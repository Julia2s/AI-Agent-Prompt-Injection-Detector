from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from src.api.v1.routes import (  # <-- Добавили health_router
    health_router,
    healthz_router,
    version_router,
)
from src.core.config import settings
from src.core.logging import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    setup_logging(level="DEBUG" if settings.DEBUG else "INFO")
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    yield
    logger.info("Shutting down application")


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
)

# Подключаем все эндпоинты
app.include_router(healthz_router)
app.include_router(version_router)
app.include_router(health_router)
