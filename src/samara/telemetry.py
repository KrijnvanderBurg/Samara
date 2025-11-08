"""OpenTelemetry telemetry setup for distributed tracing and metrics.

This module provides a simple OpenTelemetry configuration for:
1. Continuing existing traces via W3C trace context
2. Exporting traces to any OTLP-compatible backend (OTEL Collector, Jaeger, etc.)
3. Collecting and exporting metrics to any OTLP-compatible backend (OTEL Collector, Prometheus, etc.)

The design supports flexible backend configuration - use OTEL Collector as a central
aggregation point, or send directly to specific backends. Each signal (traces, metrics)
can be configured independently for maximum deployment flexibility.
"""

import logging
import platform
from os import getpid
from typing import Any

from opentelemetry import context, metrics, trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from samara import get_run_datetime, get_run_id
from samara.settings import AppSettings, get_settings
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)
settings: AppSettings = get_settings()


def setup_telemetry(
    service_name: str,
    otlp_traces_endpoint: str | None = None,
    otlp_metrics_endpoint: str | None = None,
    traceparent: str | None = None,
    tracestate: str | None = None,
) -> None:
    """Initialize OpenTelemetry with flexible OTLP exporters for traces and metrics.

    Sets up a basic telemetry configuration with:
    - Service name identification
    - OTLP HTTP exporter for traces (push-based) - configurable backend
    - OTLP HTTP exporter for metrics (push-based) - configurable backend
    - Batch span processor for efficient trace export
    - Periodic metric reader for regular metric export
    - Parent context attachment for trace continuation

    Supports flexible backend configuration:
    - Send both traces and metrics to OTEL Collector (recommended):
      traces: "https://otel-collector:4318/v1/traces"
      metrics: "https://otel-collector:4318/v1/metrics"
    - Send directly to specific backends:
      traces: "https://jaeger:4318/v1/traces"
      metrics: "https://prometheus:9090/api/v1/otlp/v1/metrics"
    - Mix and match as needed for your deployment architecture

    Args:
        service_name: Name of the service for trace identification
        otlp_traces_endpoint: OTLP endpoint URL for traces. Can be OTEL Collector,
            Jaeger, or any OTLP-compatible backend (e.g., "https://localhost:4318/v1/traces").
            If None, telemetry is configured but traces won't be exported.
        otlp_metrics_endpoint: OTLP endpoint URL for metrics. Can be OTEL Collector,
            Prometheus, or any OTLP-compatible backend (e.g., "https://localhost:4318/v1/metrics").
            If None, metrics won't be exported.
        traceparent: W3C traceparent header for continuing existing trace
        tracestate: W3C tracestate header for continuing existing trace

    Note:
        This function is idempotent - calling it multiple times will only
        initialize once. Uses OpenTelemetry's global tracer and meter providers.
        The flexible endpoint configuration allows you to adapt to different
        deployment scenarios: local development, production with OTEL Collector,
        or direct backend integration.
    """
    # Attach parent context first if provided for trace continuation
    parent_context = get_parent_context(traceparent=traceparent, tracestate=tracestate)
    if parent_context:
        context.attach(parent_context)
        logger.debug("Attached parent context for trace continuation")

    # Create shared resource for both traces and metrics
    # Add execution ID to prevent metric overwrites on CLI restarts
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.instance.id": get_run_id(),
            "service.instance.datetime": str(get_run_datetime()),
            "service.environment": str(settings.environment),
            "host.name": platform.node(),
            "host.arch": platform.machine(),
            "process.pid": str(getpid()),
        }
    )

    # Setup tracing
    trace_provider = TracerProvider(resource=resource)

    # Always add console exporter to see traces
    trace_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    # Add OTLP exporter if endpoint is provided
    if otlp_traces_endpoint:
        try:
            otlp_exporter = OTLPSpanExporter(endpoint=otlp_traces_endpoint)
            trace_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
            logger.info("Trace telemetry initialized with OTLP endpoint: %s", otlp_traces_endpoint)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("Failed to initialize OTLP trace exporter: %s", e)
    else:
        logger.info("No OTLP traces endpoint configured, traces will not be exported")

    # Set as global tracer provider (OpenTelemetry's design uses this singleton)
    trace.set_tracer_provider(trace_provider)

    # Setup metrics - use explicit endpoint or skip if not provided
    if otlp_metrics_endpoint:
        try:
            metric_exporter = OTLPMetricExporter(endpoint=otlp_metrics_endpoint)
            metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=1000)
            meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
            metrics.set_meter_provider(meter_provider)
            logger.info("Metrics telemetry initialized with OTLP endpoint: %s", otlp_metrics_endpoint)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("Failed to initialize OTLP metrics exporter: %s", e)
    else:
        logger.info("No OTLP metrics endpoint configured, metrics will not be exported")


def get_tracer(name: str = "samara") -> trace.Tracer:
    """Get a tracer instance for creating spans.

    Args:
        name: Name of the tracer (typically module or component name)

    Returns:
        Tracer instance for creating spans
    """
    return trace.get_tracer(name)


def get_meter(name: str = "samara") -> metrics.Meter:
    """Get a meter instance for creating metrics.

    Args:
        name: Name of the meter (typically module or component name)

    Returns:
        Meter instance for creating metrics (counters, histograms, etc.)
    """
    return metrics.get_meter(name)


def get_parent_context(traceparent: str | None = None, tracestate: str | None = None) -> Context | None:
    """Extract parent context from W3C trace context headers.

    This enables continuing an existing trace by parsing the traceparent
    and tracestate headers from upstream services.

    Args:
        traceparent: W3C traceparent header value
                    (e.g., "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
        tracestate: W3C tracestate header value (optional)

    Returns:
        Context object if traceparent is valid, None otherwise
    """
    if not traceparent:
        return None

    # Create carrier dict with W3C headers
    carrier: dict[str, Any] = {"traceparent": traceparent}
    if tracestate:
        carrier["tracestate"] = tracestate

    # Extract context using W3C propagator
    propagator = TraceContextTextMapPropagator()
    parent_context = propagator.extract(carrier=carrier)

    logger.debug("Extracted parent context from traceparent: %s", traceparent)
    return parent_context
