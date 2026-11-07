import importlib.metadata
import logging
import time

from fastapi import APIRouter
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from src.core.broker import pipeline_settings

router = APIRouter(prefix="/api/v1", tags=["health"])
logger = logging.getLogger(__name__)


health_engine = create_async_engine(pipeline_settings.database_url, pool_pre_ping=True)


class HealthResponse(BaseModel):
    status: str
    database_latency_ms: float | None
    dependencies: dict[str, str]


def _get_package_version(package_name: str) -> str:
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


@router.get("/health", response_model=HealthResponse, summary="End-to-end health check")
async def health_check():
    start_time = time.perf_counter()
    db_status = "disconnected"
    db_latency_ms = None

    try:
        async with health_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        db_status = "connected"
        db_latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
        logger.debug("Health check passed; DB latency: %s ms", db_latency_ms)
    except Exception:
        logger.error("Health check failed: database unavailable")
        db_latency_ms = -1.0

    dependencies = {
        "fastapi": _get_package_version("fastapi"),
        "sqlalchemy": _get_package_version("sqlalchemy"),
        "asyncpg": _get_package_version("asyncpg"),
        "database": db_status,
    }

    overall_status = "healthy" if db_status == "connected" else "unhealthy"

    return HealthResponse(
        status=overall_status,
        database_latency_ms=db_latency_ms,
        dependencies=dependencies,
    )
