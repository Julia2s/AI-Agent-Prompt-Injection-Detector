from collections.abc import AsyncIterator, Callable
from importlib import import_module
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture
def app() -> Any:
    """Load the application from the runtime entry point used by Docker."""
    project_root = Path(__file__).resolve().parents[1]
    if not (project_root / "src" / "main.py").is_file():
        pytest.skip("Backend entry point src.main:app has not been added yet.")

    module = import_module("src.main")
    application = getattr(module, "app", None)
    if application is None:
        pytest.fail("Backend module src.main does not expose an `app` object.")
    return application


@pytest.fixture
def async_client_factory() -> Callable[[Any], AsyncClient]:
    """Create an in-process HTTPX client for any ASGI application."""

    def make_client(asgi_app: Any) -> AsyncClient:
        return AsyncClient(
            transport=ASGITransport(app=asgi_app),
            base_url="http://testserver",
        )

    return make_client


@pytest.fixture
async def async_client(
    app: Any,
    async_client_factory: Callable[[Any], AsyncClient],
) -> AsyncIterator[AsyncClient]:
    async with async_client_factory(app) as client:
        yield client
