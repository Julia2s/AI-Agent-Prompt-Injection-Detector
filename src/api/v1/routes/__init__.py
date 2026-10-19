from src.api.v1.routes.health import router as health_router
from src.api.v1.routes.healthz import router as healthz_router
from src.api.v1.routes.version import router as version_router
from src.api.v1.routes.tasks import router as tasks_router

__all__ = ["healthz_router", "version_router", "health_router", "tasks_router"]
