from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(tags=["health"])

class HealthzResponse(BaseModel):
    status: str

@router.get("/healthz", response_model=HealthzResponse, summary="Liveness probe")
async def healthz():
    """
    Быстрая проверка живости процесса
    Не делает запросов к БД или внешним сервисам
    """
    return {"status": "ok"}