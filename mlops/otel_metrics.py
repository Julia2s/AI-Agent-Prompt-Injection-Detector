import atexit
import logging
import os
import socket

from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.metrics import Observation
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource

from mlops.metrics import rss_mb

INFERENCE_BUCKETS = (0.1, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 5.0)
API_BUCKETS = (0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)

_providers: list[MeterProvider] = []
logger = logging.getLogger(__name__)


def init_metrics(service: str) -> dict:
    """Создаёт MeterProvider и возвращает словарь инструментов текущего сервиса."""
    disabled = os.environ.get("OTEL_SDK_DISABLED", "true").lower() == "true"
    disabled = (
        disabled
        or os.environ.get("OTEL_METRICS_EXPORTER", "otlp").strip().lower() == "none"
    )

    readers = []
    if not disabled:
        exporter = OTLPMetricExporter(
            endpoint=os.environ.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
            or os.environ.get(
                "OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317"
            ),
            insecure=True,
        )
        readers.append(
            PeriodicExportingMetricReader(exporter, export_interval_millis=10000)
        )

    provider = MeterProvider(
        resource=Resource.create(
            {
                "service.name": os.environ.get("OTEL_SERVICE_NAME", service),
                "service.instance.id": os.environ.get("OTEL_SERVICE_INSTANCE_ID")
                or socket.gethostname(),
                "service.version": os.environ.get("OTEL_SERVICE_VERSION", "0.1.0"),
                "deployment.environment.name": os.environ.get("ENV", "dev"),
            }
        ),
        metric_readers=readers,
        shutdown_on_exit=False,
    )
    _providers.append(provider)
    meter = provider.get_meter(f"mlops.{service}")

    instruments = {
        "rss_mib": meter.create_observable_gauge(
            name="process_memory_rss_mib",
            callbacks=[lambda _o: [Observation(rss_mb(), {})]],
            unit="",
            description="Resident set size of this process",
        ),
    }

    if service == "api":
        instruments["requests_total"] = meter.create_counter(
            "api_requests_total",
            unit="1",
            description="HTTP requests by route and status code",
        )
        instruments["response_duration"] = meter.create_histogram(
            "api_response_duration_seconds",
            unit="s",
            description="API response latency",
            explicit_bucket_boundaries_advisory=list(API_BUCKETS),
        )
    elif service == "worker":
        instruments["tasks_total"] = meter.create_counter(
            "worker_tasks_total",
            unit="1",
            description="Processed tasks by result (ok/error/dlq)",
        )
        instruments["inference_duration"] = meter.create_histogram(
            "worker_inference_duration_seconds",
            unit="s",
            description="Model inference latency",
            explicit_bucket_boundaries_advisory=list(INFERENCE_BUCKETS),
        )
        instruments["inflight"] = meter.create_up_down_counter(
            "worker_inflight",
            unit="1",
            description="Tasks currently being processed",
        )
        instruments["redeliveries_total"] = meter.create_counter(
            "worker_redeliveries_total",
            unit="1",
            description="Redelivered messages",
        )
    elif service == "source":
        instruments["published_total"] = meter.create_counter(
            "source_published_total",
            unit="1",
            description="Tasks confirmed by broker",
        )
        instruments["rejected_total"] = meter.create_counter(
            "source_rejected_total",
            unit="1",
            description="Broker rejects (nack/timeout on publish)",
        )
        instruments["backoff_seconds_total"] = meter.create_counter(
            "source_backoff_seconds_total",
            unit="s",
            description="Cumulative backoff wait time",
        )
    return instruments


def shutdown_metrics() -> None:
    """Flush and close every metrics provider once."""
    while _providers:
        provider = _providers.pop()

        try:
            provider.force_flush(timeout_millis=5000)
        except Exception:
            logger.warning(
                "Metrics flush failed during shutdown",
                exc_info=True,
            )
        finally:
            try:
                provider.shutdown(timeout_millis=5000)
            except Exception:
                logger.warning(
                    "Metrics provider shutdown failed",
                    exc_info=True,
                )


atexit.register(shutdown_metrics)
