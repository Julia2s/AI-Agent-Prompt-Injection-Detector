import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_healthz_success(async_client: AsyncClient):
    response = await async_client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
