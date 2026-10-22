import pytest
from httpx import AsyncClient

from src.core.broker import pipeline_settings


@pytest.mark.asyncio
async def test_version_endpoint(async_client: AsyncClient):
    response = await async_client.get("/api/v1/version")
    assert response.status_code == 200
    data = response.json()
    assert data["app_name"] == pipeline_settings.app_name
    assert data["version"] == pipeline_settings.app_version
