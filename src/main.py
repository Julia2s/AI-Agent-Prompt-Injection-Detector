import mlflow
import mlflow.sklearn
from contextlib import asynccontextmanager

from fastapi import FastAPI
from loguru import logger

from src.api.v1.routes import (
    health_router,
    healthz_router,
    version_router,
    process_router,
)
from src.core.config import settings
from src.core.logging import setup_logging


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    setup_logging(level="DEBUG" if settings.DEBUG else "INFO")
    logger.info(f"Starting {settings.APP_NAME} v{settings.APP_VERSION}")
    
    app.state.model_pipeline = None
    
    mlflow.set_tracking_uri(settings.MLFLOW_TRACKING_URI)
    logger.info(f"MLflow Tracking URI set to: {settings.MLFLOW_TRACKING_URI}")
    
    model_uri = f"models:/{settings.MODEL_NAME}@{settings.MODEL_ALIAS}"
    logger.info(f"Loading model from MLflow: {model_uri}")
    
    try:
        app.state.model_pipeline = mlflow.sklearn.load_model(model_uri)
        logger.info("Model successfully loaded into memory!")
    except Exception as e:
        logger.error(f"Failed to load model: {e}")
    
    yield 
    
    logger.info("Shutting down application, clearing model from memory")
    app.state.model_pipeline = None


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    lifespan=lifespan,
)

app.include_router(healthz_router)
app.include_router(version_router)
app.include_router(health_router)
app.include_router(process_router)
