from fastapi import APIRouter
from pydantic import BaseModel

from src.core.config import settings

router = APIRouter(prefix="/api/v1", tags=["info"])

class VersionResponse(BaseModel):
    app_name: str
    version: str

@router.get("/version", response_model=VersionResponse, summary="Get app version")
async def get_version():
    """ Возвращает текущую версию и имя приложения из конфига """
    return {
        "app_name": settings.APP_NAME,
        "version": settings.APP_VERSION,
    }