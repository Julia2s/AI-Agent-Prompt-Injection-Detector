"""One topology and publisher-confirm policy for every process."""

from faststream.rabbit import Channel, RabbitBroker, RabbitQueue

from mlops.settings import PipelineSettings


def task_queue(settings: PipelineSettings):
    return RabbitQueue(
        settings.queue_name,
        durable=True,
        arguments={
            "x-max-length": settings.queue_capacity,
            "x-overflow": "reject-publish",
        },
    )


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
        graceful_timeout=None,
    )


async def declare_topology(broker, settings):
    await broker.declare_queue(dead_queue(settings))
    await broker.declare_queue(task_queue(settings))


async def publish_task(broker, settings, task):
    await broker.publish(
        task,
        queue=task_queue(settings),
        persist=True,
        mandatory=True,
        timeout=settings.publish_timeout,
        message_id=task["task_id"],
    )
