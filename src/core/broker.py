import os
from faststream.rabbit import RabbitBroker

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")

broker = RabbitBroker(RABBITMQ_URL)