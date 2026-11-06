import pytest
from httpx import ASGITransport, AsyncClient


@pytest.mark.asyncio
async def test_readiness_and_inference(app):
    class Model:
        classes_ = [1, 0]

        def predict(self, texts):
            return [1]

        def predict_proba(self, texts):
            return [[0.8, 0.2]]

    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        app.state.model_pipeline = None
        assert (await client.get("/api/v1/ready")).status_code == 503
        assert (
            await client.post("/api/v1/process", json={"text": "ok"})
        ).status_code == 503
        assert (
            await client.post("/api/v1/process", json={"text": "  "})
        ).status_code == 422
        assert (
            await client.post("/api/v1/process", json={"text": "x" * 10001})
        ).status_code == 422
        app.state.model_pipeline = Model()
        assert (await client.get("/api/v1/ready")).status_code == 200
        response = await client.post("/api/v1/process", json={"text": "ok"})
        assert response.json()["probability"] == 0.8

        def fail(texts):
            raise RuntimeError("secret detail")

        app.state.model_pipeline.predict = fail
        response = await client.post("/api/v1/process", json={"text": "ok"})
        assert response.status_code == 500
        assert "secret" not in response.text
        app.state.model_pipeline = None
