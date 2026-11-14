"""OpenTelemetry SDK setup shared by the API, source, and worker processes."""

import os
import socket
from threading import Lock

from opentelemetry import trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.propagate import set_global_textmap
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import (
    ALWAYS_OFF,
    ALWAYS_ON,
    ParentBased,
    TraceIdRatioBased,
)
from opentelemetry.trace import TracerProvider as TracerProviderProtocol
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

_lock = Lock()
_provider: TracerProvider | None = None
_configured = False


def _truthy(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _resource_attributes(service_name: str) -> dict[str, str]:
    attributes: dict[str, str] = {}
    for item in os.getenv("OTEL_RESOURCE_ATTRIBUTES", "").split(","):
        key, separator, value = item.partition("=")
        if separator and key.strip() and value.strip():
            attributes[key.strip()] = value.strip()

    attributes.setdefault("service.version", os.getenv("APP_VERSION", "0.1.0"))
    attributes.setdefault(
        "deployment.environment.name", os.getenv("ENV", "dev")
    )
    attributes.setdefault(
        "service.instance.id",
        os.getenv("OTEL_SERVICE_INSTANCE_ID") or socket.gethostname(),
    )
    attributes["service.name"] = os.getenv("OTEL_SERVICE_NAME", service_name)
    return attributes


def _sampler():
    sampler_name = os.getenv(
        "OTEL_TRACES_SAMPLER", "parentbased_always_on"
    ).strip().lower()
    sampler_arg = float(os.getenv("OTEL_TRACES_SAMPLER_ARG", "1.0"))
    if not 0.0 <= sampler_arg <= 1.0:
        raise ValueError("OTEL_TRACES_SAMPLER_ARG must be between 0 and 1")

    samplers = {
        "always_on": ALWAYS_ON,
        "always_off": ALWAYS_OFF,
        "traceidratio": TraceIdRatioBased(sampler_arg),
        "parentbased_always_on": ParentBased(root=ALWAYS_ON),
        "parentbased_always_off": ParentBased(root=ALWAYS_OFF),
        "parentbased_traceidratio": ParentBased(
            root=TraceIdRatioBased(sampler_arg)
        ),
    }
    try:
        return samplers[sampler_name]
    except KeyError as error:
        supported = ", ".join(sorted(samplers))
        raise ValueError(
            f"Unsupported OTEL_TRACES_SAMPLER {sampler_name!r}; "
            f"choose one of: {supported}"
        ) from error


def configure_tracing(service_name: str) -> TracerProviderProtocol | None:
    """Configure OTLP tracing once, or leave tracing as a no-op when disabled."""
    global _configured, _provider

    with _lock:
        if _configured:
            return _provider

        endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "").strip()
        if _truthy("OTEL_SDK_DISABLED") or not endpoint:
            return None

        _configured = True
        set_global_textmap(
            CompositePropagator(
                [TraceContextTextMapPropagator(), W3CBaggagePropagator()]
            )
        )
        provider = TracerProvider(
            resource=Resource.create(_resource_attributes(service_name)),
            sampler=_sampler(),
        )
        exporter = OTLPSpanExporter(
            endpoint=endpoint,
            timeout=float(os.getenv("OTEL_EXPORTER_OTLP_TIMEOUT", "5")),
        )
        provider.add_span_processor(BatchSpanProcessor(exporter))
        trace.set_tracer_provider(provider)
        _provider = provider
        return provider


def current_provider() -> TracerProviderProtocol | None:
    return _provider


def shutdown_tracing() -> None:
    """Flush briefly on shutdown; an unavailable Collector must not block exit."""
    if _provider is not None:
        _provider.force_flush(timeout_millis=1000)
        _provider.shutdown()
