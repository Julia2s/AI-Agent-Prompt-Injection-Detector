import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from mlops.messaging import declare_topology
from mlops.otel_metrics import init_metrics
from src.api.v1.routes import (
    health_router,
    healthz_router,
    tasks_router,
    version_router,
)
from src.api.v1.routes.tasks import store
from src.core.broker import broker, pipeline_settings
from src.core.logging import setup_logging
from src.core.tracing import current_provider, shutdown_tracing

setup_logging("api")
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
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
    await broker.stop()
    await store.close()
    shutdown_tracing()
    logger.info("API shutdown completed")


app = FastAPI(
    title=pipeline_settings.app_name,
    version=pipeline_settings.app_version,
    lifespan=lifespan,
)

FastAPIInstrumentor.instrument_app(
    app,
    tracer_provider=current_provider(),
    excluded_urls="/healthz,/api/v1/health",
)

METRICS = init_metrics("api")


@app.middleware("http")
async def metrics_middleware(request, call_next):
    started = time.perf_counter()
    status_code = None

    try:
        response = await call_next(request)
        status_code = response.status_code
        return response
    except Exception:
        status_code = 500
        raise
    finally:
        route = request.scope.get("route")
        path = getattr(route, "path", "unmatched")

        if status_code is not None and path not in ("/healthz", "/api/v1/health"):
            METRICS["requests_total"].add(
                1,
                {"route": path, "status_code": str(status_code)},
            )
            METRICS["response_duration"].record(
                time.perf_counter() - started,
                {"route": path},
            )


app.include_router(healthz_router)
app.include_router(version_router)
app.include_router(health_router)
app.include_router(tasks_router)
