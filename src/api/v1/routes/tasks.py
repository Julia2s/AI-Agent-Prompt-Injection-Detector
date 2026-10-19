import uuid
import logging
from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel

from src.core.broker import broker

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["tasks"])

class TaskRequest(BaseModel):
    text: str
    task_id: str | None = None

@router.post("/tasks", status_code=202)
async def create_task(request: TaskRequest, response: Response):
    task_id = request.task_id or str(uuid.uuid4())
    
    try:
        await broker.publish(
            {"task_id": task_id, "text": request.text},
            queue="inference_queue",
            timeout=1.0
        )
        
        logger.info(f"📤 Задача {task_id} успешно поставлена в очередь")
        return {
            "task_id": task_id,
            "status": "ACCEPTED",
            "message": "Задача принята в асинхронную обработку"
        }
        
    except Exception as e:
        logger.warning(f"Перегрузка системы! Задача {task_id} отклонена: {e}")
        response.headers["Retry-After"] = "5"
        raise HTTPException(
            status_code=503, 
            detail="Service Unavailable: Система перегружена, попробуйте позже"
        )