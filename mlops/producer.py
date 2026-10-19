# mlops/producer.py
import os
import asyncio
import uuid
import logging
from faststream.rabbit import RabbitBroker, RabbitQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
TASKS_COUNT = 5000 

async def main():
    async with RabbitBroker(RABBITMQ_URL) as broker:
        logger.info(f"Запуск источника задач. Планируется отправить {TASKS_COUNT} задач...")
        
        target_queue = RabbitQueue(
            name="inference_queue",
            durable=True,
            arguments={
                "x-max-length": 50,
                "x-overflow": "reject-publish"
            }
        )
        
        failed_count = 0
        for i in range(TASKS_COUNT):
            task_id = str(uuid.uuid4())
            text = f"Тестовая задача {i}"
            
            try:
                await broker.publish(
                    {"task_id": task_id, "text": text},
                    queue=target_queue,
                    timeout=2.0
                )

                if failed_count > 0:
                    delay = min(0.1 * (2 ** failed_count), 2.0)
                    await asyncio.sleep(delay)
                    failed_count = 0
                    
            except Exception as e:
                logger.warning(f"Очередь переполнена! Задача {i} отклонена брокером. Замедляемся... (Ошибка: {type(e).__name__})")
                failed_count += 1

                await asyncio.sleep(0.5)

                continue 
            
            if (i + 1) % 500 == 0:
                logger.info(f"Успешно отправлено {i + 1} задач...")
                
        logger.info("Источник задач завершил работу.")

if __name__ == "__main__":
    asyncio.run(main())