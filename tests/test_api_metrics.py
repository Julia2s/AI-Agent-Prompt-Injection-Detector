from unittest.mock import Mock

import pytest
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from src import main


@pytest.mark.parametrize("status_code", [200, 503, 500])
async def test_api_metrics_record_response(monkeypatch, status_code):
    metrics = {"requests_total": Mock(), "response_duration": Mock()}
    monkeypatch.setattr(main, "METRICS", metrics)

    app = FastAPI()
    app.middleware("http")(main.metrics_middleware)

    @app.get("/probe")
    async def probe():
        if status_code == 500:
            raise RuntimeError("test failure")
        if status_code == 503:
            raise HTTPException(status_code=503)
        return {"ok": True}

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/probe")

    assert response.status_code == status_code
    metrics["requests_total"].add.assert_called_once_with(
        1, {"route": "/probe", "status_code": str(status_code)}
    )
    metrics["response_duration"].record.assert_called_once()


async def test_unknown_urls_share_one_route_label(monkeypatch):
    metrics = {"requests_total": Mock(), "response_duration": Mock()}
    monkeypatch.setattr(main, "METRICS", metrics)

    app = FastAPI()
    app.middleware("http")(main.metrics_middleware)

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        for path in ("/unknown/1", "/unknown/2"):
            response = await client.get(path)
            assert response.status_code == 404

    calls = metrics["requests_total"].add.call_args_list
    assert len(calls) == 2
    assert all(
        call.args == (1, {"route": "unmatched", "status_code": "404"}) for call in calls
    )
