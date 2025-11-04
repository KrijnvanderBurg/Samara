"""OpenTelemetry telemetry setup for distributed tracing.

This module provides a simple OpenTelemetry configuration for:
1. Continuing existing traces via W3C trace context
2. Exporting traces to OTLP endpoints via push

Keep it basic and simple - just the essentials for distributed tracing.
"""

import logging
from typing import Any

from opentelemetry import context, trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)


def setup_telemetry(
    service_name: str,
    otlp_endpoint: str | None = None,
    traceparent: str | None = None,
    tracestate: str | None = None,
) -> None:
    """Initialize OpenTelemetry with OTLP exporter.

    Sets up a basic telemetry configuration with:
    - Service name identification
    - OTLP gRPC exporter (push-based)
    - Batch span processor for efficient export
    - Parent context attachment for trace continuation

    Args:
        service_name: Name of the service for trace identification
        otlp_endpoint: OTLP endpoint URL (e.g., "http://localhost:4318/v1/traces")
                      If None, telemetry is configured but traces won't be exported
        traceparent: W3C traceparent header for continuing existing trace
        tracestate: W3C tracestate header for continuing existing trace

    Note:
        This function is idempotent - calling it multiple times will only
        initialize once. Uses OpenTelemetry's global tracer provider.
    """
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    # Always add console exporter to see traces
    provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    # Add OTLP exporter if endpoint is provided
    if otlp_endpoint:
        try:
            otlp_exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
            provider.add_span_processor(BatchSpanProcessor(otlp_exporter))
            logger.info("Telemetry initialized with OTLP endpoint: %s", otlp_endpoint)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("Failed to initialize OTLP exporter: %s", e)
    else:
        logger.info("No OTLP endpoint configured, traces will not be exported")

    # Set as global tracer provider (OpenTelemetry's design uses this singleton)
    trace.set_tracer_provider(provider)

    # Attach parent context if provided for trace continuation
    parent_context = get_parent_context(traceparent=traceparent, tracestate=tracestate)
    if parent_context:
        context.attach(parent_context)
        logger.debug("Attached parent context for trace continuation")


def get_tracer(name: str = "samara") -> trace.Tracer:
    """Get a tracer instance for creating spans.

    Args:
        name: Name of the tracer (typically module or component name)

    Returns:
        Tracer instance for creating spans
    """
    return trace.get_tracer(name)


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
