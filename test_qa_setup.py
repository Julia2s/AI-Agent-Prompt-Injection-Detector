from typing import Any

from httpx import AsyncClient


async def _ok_asgi_app(
    scope: dict[str, Any],
    receive: Any,
    send: Any,
) -> None:
    if scope["type"] != "http":
        return

    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        }
    )
    await send({"type": "http.response.body", "body": b'{"status":"ok"}'})


async def test_async_client_factory_supports_asgi_apps(
    async_client_factory: Any,
) -> None:
    async with async_client_factory(_ok_asgi_app) as client:
        assert isinstance(client, AsyncClient)
        response = await client.get("/")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
