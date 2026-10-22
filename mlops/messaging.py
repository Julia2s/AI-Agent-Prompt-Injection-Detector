"""One topology and publisher-confirm policy for every process."""

import time

from faststream.rabbit import Channel, RabbitBroker, RabbitQueue

from mlops.settings import PipelineSettings


def task_queue(settings: PipelineSettings):
    arguments = {}
    if settings.bounded:
        arguments = {
            "x-max-length": settings.queue_capacity,
            "x-overflow": "reject-publish",
        }
    return RabbitQueue(settings.queue_name, durable=True, arguments=arguments)


def dead_queue(settings: PipelineSettings):
    return RabbitQueue(settings.queue_name + ".dlq", durable=True)


def make_broker(settings: PipelineSettings):
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(
            publisher_confirms=True,
            on_return_raises=True,
            prefetch_count=settings.prefetch_count,
        ),
        graceful_timeout=settings.graceful_timeout,
    )


async def declare_topology(broker, settings):
    await broker.declare_queue(dead_queue(settings))
    await broker.declare_queue(task_queue(settings))


def new_task(task_id: str, text: str, **extra) -> dict:
    """Message body. `created_at` lets the worker measure end-to-end latency."""
    return {"task_id": task_id, "text": text, "created_at": time.time(), **extra}


async def publish_task(broker, settings, task):
    await broker.publish(
        task,
        queue=task_queue(settings),
        persist=True,
        mandatory=True,
        timeout=settings.publish_timeout,
        message_id=task["task_id"],
    )
