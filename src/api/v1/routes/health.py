import time
import importlib.metadata
from fastapi import APIRouter
from pydantic import BaseModel
from loguru import logger

from src.core.database import engine

router = APIRouter(prefix="/api/v1", tags=["health"])


class HealthResponse(BaseModel):
    status: str
    database_latency_ms: float | None
    dependencies: dict[str, str]


def _get_package_version(package_name: str) -> str:
    """ Безопасно получает версию установленного пакета """
    try:
        return importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        return "unknown"


@router.get("/health", response_model=HealthResponse, summary="End-to-end health check")
async def health_check():
    """
    Проверка работоспособности приложения и подключений
    Замеряет латентность БД и собирает версии зависимостей
    """
    start_time = time.perf_counter()
    db_status = "disconnected"
    db_latency_ms = None

    try:
        # Легковесный запрос для проверки пула соединений
        async with engine.connect() as conn:
            await conn.execute("SELECT 1")
        
        db_status = "connected"
        db_latency_ms = round((time.perf_counter() - start_time) * 1000, 2)
        logger.info(f"Health check PASSED. DB latency: {db_latency_ms}ms")
        
    except Exception as e:
        logger.error(f"Health check FAILED. DB connection error: {e}")
        db_latency_ms = -1.0

    # Собираем версии ключевых зависимостей
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