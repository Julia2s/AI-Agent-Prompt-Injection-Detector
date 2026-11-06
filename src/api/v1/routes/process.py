from fastapi import APIRouter, HTTPException, Request
from loguru import logger
from pydantic import BaseModel, Field, field_validator

from src.core.config import settings

router = APIRouter(prefix="/api/v1", tags=["inference"])


class ProcessRequest(BaseModel):
    text: str = Field(min_length=1, max_length=10000)

    @field_validator("text")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Text must not be blank")
        return value


class ProcessResponse(BaseModel):
    is_injection: bool
    probability: float
    model_name: str
    model_alias: str


@router.post(
    "/process", response_model=ProcessResponse, summary="Detect prompt injection"
)
async def process_prompt(request: Request, payload: ProcessRequest):
    model_pipeline = getattr(request.app.state, "model_pipeline", None)

    if model_pipeline is None:
        raise HTTPException(
            status_code=503, detail="Model is not loaded. Check application logs."
        )

    try:
        prediction = model_pipeline.predict([payload.text])[0]
        probabilities = model_pipeline.predict_proba([payload.text])[0]
        classes = list(model_pipeline.classes_)
        if len(classes) != len(probabilities) or 1 not in classes:
            raise ValueError("Invalid model probability classes")
        prob_injection = float(probabilities[classes.index(1)])
        if not 0 <= prob_injection <= 1:
            raise ValueError("Invalid model probability")

        return ProcessResponse(
            is_injection=bool(prediction),
            probability=round(prob_injection, 4),
            model_name=settings.MODEL_NAME,
            model_alias=settings.MODEL_ALIAS,
        )
    except Exception:
        logger.exception("Inference failed")
        raise HTTPException(status_code=500, detail="Inference failed") from None
