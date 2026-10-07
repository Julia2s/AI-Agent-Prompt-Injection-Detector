from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from src.core.config import settings

router = APIRouter(prefix="/api/v1", tags=["inference"])


class ProcessRequest(BaseModel):
    text: str


class ProcessResponse(BaseModel):
    is_injection: bool
    probability: float
    model_name: str
    model_alias: str


@router.post("/process", response_model=ProcessResponse, summary="Detect prompt injection")
async def process_prompt(request: Request, payload: ProcessRequest):
    model_pipeline = request.app.state.model_pipeline
    
    if model_pipeline is None:
        raise HTTPException(status_code=503, detail="Model is not loaded. Check application logs.")

    try:
        prediction = model_pipeline.predict([payload.text])[0]
        probabilities = model_pipeline.predict_proba([payload.text])[0]
        prob_injection = float(probabilities[1])
        
        return ProcessResponse(
            is_injection=bool(prediction),
            probability=round(prob_injection, 4),
            model_name=settings.MODEL_NAME,
            model_alias=settings.MODEL_ALIAS,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Inference error: {str(e)}")