import uuid

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from mlops.messaging import publish_task
from mlops.producer import TRANSIENT_ERRORS
from src.core.broker import broker, pipeline_settings

router = APIRouter(prefix="/api/v1", tags=["tasks"])


class TaskRequest(BaseModel):
    text: str = Field(min_length=1, max_length=100000)
    task_id: str | None = Field(default=None, min_length=1, max_length=128)


@router.post("/tasks", status_code=202)
async def create_task(request: TaskRequest):
    task_id = request.task_id or str(uuid.uuid4())
    try:
        await publish_task(
            broker, pipeline_settings, {"task_id": task_id, "text": request.text}
        )
    except TRANSIENT_ERRORS as error:
        raise HTTPException(
            status_code=503,
            detail={
                "message": "Queue unavailable; retry with the same task_id",
                "task_id": task_id,
            },
            headers={"Retry-After": str(pipeline_settings.retry_after)},
        ) from error
    return {"task_id": task_id, "status": "ACCEPTED"}
