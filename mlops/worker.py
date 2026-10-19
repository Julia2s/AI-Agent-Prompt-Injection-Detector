import os
import time
import asyncio
import mlflow
import logging
from faststream import FastStream
from faststream.rabbit import RabbitBroker, RabbitQueue
from pydantic import BaseModel
from contextlib import asynccontextmanager

from mlops.db import init_db, async_session, TaskResult

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_ALIAS = "models:/prompt-injection-detector@champion"

INFERENCE_DELAY = float(os.getenv("INFERENCE_DELAY", "0.5")) 

broker = RabbitBroker(RABBITMQ_URL)

model = None

@asynccontextmanager
async def lifespan():
    global model
    logger.info("Запуск воркера. Инициализация БД...")
    await init_db()
    
    logger.info(f"Загрузка модели из MLflow по алиасу: {MODEL_ALIAS}")
    mlflow.set_tracking_uri(MLFLOW_URI)

    model = mlflow.pyfunc.load_model(MODEL_ALIAS)
    logger.info("Модель успешно загружена в память!")
    
    yield
    
    logger.info("Остановка воркера...")

app = FastStream(broker, lifespan=lifespan)

class TaskMessage(BaseModel):
    task_id: str
    text: str

@broker.subscriber(
    queue=RabbitQueue(
        name="inference_queue", 
        durable=True,
        arguments={
            "x-max-length": 50,
            "x-overflow": "reject-publish"
        }
    ),
)
async def process_task(msg: TaskMessage):
    logger.info(f"Получена задача: {msg.task_id}")
    
    try:
        def run_inference():
            time.sleep(INFERENCE_DELAY)
            prediction = model.predict([msg.text])
            return prediction[0] if isinstance(prediction, (list, tuple)) else prediction

        is_injection = await asyncio.to_thread(run_inference)

        async with async_session() as session:
            result = TaskResult(
                task_id=msg.task_id,
                text=msg.text,
                is_injection=bool(is_injection)
            )
            await session.merge(result)
            await session.commit()
            
        logger.info(f"Задача {msg.task_id} выполнена и сохранена. Инъекция: {is_injection}")
        
    except Exception as e:
        logger.error(f"Ошибка обработки задачи {msg.task_id}: {e}")
        raise