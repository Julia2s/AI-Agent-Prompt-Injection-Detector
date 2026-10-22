from fastapi import APIRouter
from pydantic import BaseModel

from src.core.broker import pipeline_settings

router = APIRouter(prefix="/api/v1", tags=["info"])


class VersionResponse(BaseModel):
    app_name: str
    version: str


@router.get("/version", response_model=VersionResponse, summary="Get app version")
async def get_version():
    return {
        "app_name": pipeline_settings.app_name,
        "version": pipeline_settings.app_version,
    }
