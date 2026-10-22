"""Task submission API.

System boundary: an external HTTP client cannot be slowed down, so when the
pipeline is saturated we do NOT wait for a free slot — we answer 503 immediately
(load shedding) and leave nothing behind.

POST /api/v1/tasks lifecycle:
  1. register the task as PENDING (so GET /tasks/{id} is meaningful),
  2. publish with a publisher confirm (bounded by PUBLISH_TIMEOUT, no wait loop),
  3. confirm      -> 202 + Location,
     NACK/timeout -> delete the PENDING row (compensation) and answer 503.
"""

import contextlib
import uuid
from datetime import datetime

from fastapi import APIRouter, HTTPException, Path, Response
from pydantic import BaseModel, Field
from sqlalchemy.exc import SQLAlchemyError

from mlops.db import SUCCESS, ResultStore
from mlops.messaging import new_task, publish_task
from mlops.producer import TRANSIENT_ERRORS
from src.core.broker import broker, pipeline_settings

router = APIRouter(prefix="/api/v1", tags=["tasks"])
store = ResultStore(pipeline_settings.database_url)


class TaskRequest(BaseModel):
    text: str = Field(min_length=1, max_length=100000, description="Prompt to scan")
    task_id: str | None = Field(
        default=None,
        min_length=1,
        max_length=128,
        description="Idempotency key. Reuse it when retrying after a 503.",
    )


class TaskView(BaseModel):
    task_id: str
    status: str = Field(description="PENDING or SUCCESS")
    is_injection: bool | None = None
    label: str | None = Field(default=None, description="injection / benign")
    created_at: datetime | None = None
    finished_at: datetime | None = None
    inference_ms: float | None = None
    latency_ms: float | None = None
    status_url: str


def to_view(row) -> TaskView:
    label = None
    if row.is_injection is not None:
        label = "injection" if row.is_injection else "benign"
    return TaskView(
        task_id=row.task_id,
        status=row.status,
        is_injection=row.is_injection,
        label=label,
        created_at=row.created_at,
        finished_at=row.finished_at,
        inference_ms=row.inference_ms,
        latency_ms=row.latency_ms,
        status_url=f"/api/v1/tasks/{row.task_id}",
    )


def overloaded(task_id: str, reason: str):
    retry_after = pipeline_settings.retry_after
    return HTTPException(
        status_code=503,
        detail={
            "error": "service_overloaded",
            "message": reason,
            "task_id": task_id,
            "retry_after_seconds": retry_after,
        },
        headers={"Retry-After": str(retry_after)},
    )


@router.post(
    "/tasks",
    status_code=202,
    response_model=TaskView,
    responses={
        200: {"description": "Task with this id already finished (idempotent retry)"},
        409: {"description": "task_id already used with a different text"},
        503: {"description": "Pipeline saturated; retry later with the same task_id"},
    },
)
async def create_task(request: TaskRequest, response: Response):
    task_id = request.task_id or str(uuid.uuid4())
    try:
        row, created = await store.create_pending(task_id, request.text)
    except (SQLAlchemyError, OSError) as error:
        raise overloaded(task_id, "Result storage is unavailable") from error
    if row.text != request.text:
        raise HTTPException(
            status_code=409,
            detail={"error": "task_id_conflict", "task_id": task_id},
        )
    if row.status == SUCCESS:
        response.status_code = 200
        return to_view(row)
    try:
        await publish_task(
            broker,
            pipeline_settings,
            new_task(task_id, request.text),
        )
    except TRANSIENT_ERRORS as error:
        if created:
            # If even the compensation fails, a stale PENDING remains; the
            # client's retry with the same task_id re-publishes it safely.
            with contextlib.suppress(SQLAlchemyError, OSError):
                await store.discard_pending(task_id)
        raise overloaded(task_id, "Queue is full or unavailable") from error
    response.headers["Location"] = f"/api/v1/tasks/{task_id}"
    return to_view(row)


@router.get("/tasks/{task_id}", response_model=TaskView)
async def get_task(task_id: str = Path(min_length=1, max_length=128)):
    try:
        row = await store.get(task_id)
    except (SQLAlchemyError, OSError) as error:
        raise overloaded(task_id, "Result storage is unavailable") from error
    if row is None:
        raise HTTPException(status_code=404, detail="Unknown task_id")
    return to_view(row)
