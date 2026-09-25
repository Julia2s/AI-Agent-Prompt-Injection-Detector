from unittest.mock import AsyncMock, MagicMock

import pytest
from httpx import AsyncClient

import src.api.v1.routes.health as health_module


@pytest.mark.asyncio
async def test_health_success(async_client: AsyncClient, monkeypatch):
    mock_conn = AsyncMock()
    mock_conn.execute = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=None)

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(return_value=mock_conn)

    monkeypatch.setattr(health_module, "engine", mock_engine)

    response = await async_client.get("/api/v1/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["database_latency_ms"] >= 0
    assert "fastapi" in data["dependencies"]


@pytest.mark.asyncio
async def test_health_db_failure(async_client: AsyncClient, monkeypatch):
    mock_conn = AsyncMock()
    mock_conn.__aenter__ = AsyncMock(
        side_effect=Exception("Database connection failed")
    )
    mock_conn.__aexit__ = AsyncMock(return_value=None)

    mock_engine = MagicMock()
    mock_engine.connect = MagicMock(return_value=mock_conn)

    monkeypatch.setattr(health_module, "engine", mock_engine)

    response = await async_client.get("/api/v1/health")

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "unhealthy"
    assert data["database_latency_ms"] == -1.0
    assert data["dependencies"]["database"] == "disconnected"
