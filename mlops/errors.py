from aio_pika.exceptions import AMQPException

TRANSIENT_ERRORS = (
    AMQPException,
    TimeoutError,
    ConnectionError,
    OSError,
)
