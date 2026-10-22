"""HTTP boundary: 202 / 200 / 409 / 503 contract and no PENDING ghosts."""

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from mlops.db import ResultStore
from src.api.v1.routes import tasks


@pytest.fixture
async def api(tmp_path, monkeypatch):
    store = ResultStore(f"sqlite+aiosqlite:///{tmp_path}/api.db")
    await store.initialize()
    publish = AsyncMock()
    monkeypatch.setattr(tasks, "store", store)
    monkeypatch.setattr(tasks, "publish_task", publish)
    app = FastAPI()
    app.include_router(tasks.router)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        client.publish = publish
        client.store = store
        yield client
    await store.close()


async def test_accepts_validates_and_returns_location(api):
    response = await api.post("/api/v1/tasks", json={"text": "hi", "task_id": "t1"})
    assert response.status_code == 202
    assert response.headers["Location"] == "/api/v1/tasks/t1"
    assert response.json()["status"] == "PENDING"
    assert (await api.post("/api/v1/tasks", json={"text": ""})).status_code == 422
    assert api.publish.await_count == 1
    body = api.publish.await_args.args[2]
    assert body["task_id"] == "t1" and "created_at" in body


async def test_status_endpoint_follows_lifecycle(api):
    assert (await api.get("/api/v1/tasks/nope")).status_code == 404
    await api.post("/api/v1/tasks", json={"text": "hi", "task_id": "t2"})
    assert (await api.get("/api/v1/tasks/t2")).json()["status"] == "PENDING"
    await api.store.save("t2", "hi", True, 12.5, 40.0)  # what the worker does
    done = (await api.get("/api/v1/tasks/t2")).json()
    assert done["status"] == "SUCCESS" and done["label"] == "injection"
    assert done["inference_ms"] == 12.5


async def test_overload_is_immediate_503_with_retry_after_and_no_pending(api):
    api.publish.side_effect = TimeoutError()
    response = await api.post("/api/v1/tasks", json={"task_id": "x", "text": "hi"})
    assert response.status_code == 503
    assert int(response.headers["Retry-After"]) == tasks.pipeline_settings.retry_after
    assert response.json()["detail"]["task_id"] == "x"
    assert api.publish.await_count == 1  # one attempt, no waiting loop
    assert await api.store.get("x") is None  # rolled back: no PENDING ghost
    assert (await api.get("/api/v1/tasks/x")).status_code == 404


async def test_retry_after_503_with_same_task_id_succeeds(api):
    api.publish.side_effect = [TimeoutError(), None]
    first = await api.post("/api/v1/tasks", json={"task_id": "r", "text": "hi"})
    second = await api.post("/api/v1/tasks", json={"task_id": "r", "text": "hi"})
    assert (first.status_code, second.status_code) == (503, 202)


async def test_repeat_of_finished_task_returns_result_without_publishing(api):
    await api.store.create_pending("d", "hi")
    await api.store.save("d", "hi", False)
    response = await api.post("/api/v1/tasks", json={"task_id": "d", "text": "hi"})
    assert response.status_code == 200 and response.json()["label"] == "benign"
    api.publish.assert_not_awaited()


async def test_task_id_reuse_with_other_text_is_conflict(api):
    await api.post("/api/v1/tasks", json={"task_id": "c", "text": "one"})
    response = await api.post("/api/v1/tasks", json={"task_id": "c", "text": "two"})
    assert response.status_code == 409


async def test_failed_republish_keeps_older_pending_row(api):
    await api.post("/api/v1/tasks", json={"task_id": "k", "text": "hi"})
    api.publish.side_effect = TimeoutError()
    response = await api.post("/api/v1/tasks", json={"task_id": "k", "text": "hi"})
    assert response.status_code == 503
    # The first message is already in the queue: its PENDING row must survive.
    assert (await api.store.get("k")).status == "PENDING"
