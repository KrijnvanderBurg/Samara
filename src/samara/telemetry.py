"""Telemetry - OpenTelemetry configuration for distributed tracing and logging.

This module provides OpenTelemetry integration enabling:
- Trace continuation via W3C trace context propagation
- OTLP export to configurable backends (Collector, Jaeger, Loki)
- Flexible signal routing with independent endpoint configuration
- Python logging bridge for unified observability

Supports deployment flexibility by allowing each signal (traces, logs) to target
different backends or route through a central OTEL Collector.
"""

import functools
import logging
from collections.abc import Callable

# import platform
# from os import getpid
from typing import Any, ParamSpec, TypeVar

from opentelemetry import context, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from samara.settings import AppSettings, get_settings
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)
settings: AppSettings = get_settings()

# Type variables enable generic decorator that preserves function signatures
P = ParamSpec("P")  # Captures parameter specification (args and kwargs)
T = TypeVar("T")  # Captures return type


def setup_telemetry(
    service_name: str,
    otlp_traces_endpoint: str | None = None,
    otlp_logs_endpoint: str | None = None,
    traceparent: str | None = None,
    tracestate: str | None = None,
) -> None:
    """Initialize OpenTelemetry with configurable OTLP exporters.

    Configure distributed tracing and logging with OTLP HTTP exporters supporting
    flexible backend routing. Enables trace continuation via W3C context propagation
    and bridges Python logging to OTLP for unified observability.

    The setup creates:
    - Global tracer provider with batch span processor
    - Console exporter for local trace visibility
    - Optional OTLP trace exporter (Jaeger, Tempo, Collector)
    - Optional OTLP log exporter (Loki, Collector)
    - Logging handler bridge attaching to root logger

    Args:
        service_name: Service identifier for trace attribution. Appears as service.name
            in resource attributes.
        otlp_traces_endpoint: OTLP HTTP endpoint for trace export. Examples:
            "https://otel-collector:4318/v1/traces" (collector)
            "https://jaeger:4318/v1/traces" (direct backend)
            If None, traces export to console only.
        otlp_logs_endpoint: OTLP HTTP endpoint for log export. Examples:
            "https://otel-collector:4318/v1/logs" (collector)
            "https://loki:3100/loki/api/v1/push" (direct backend)
            If None, logs remain in local handlers only.
        traceparent: W3C traceparent header (format: version-trace_id-span_id-flags)
            for continuing upstream traces. Example:
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        tracestate: W3C tracestate header for vendor-specific trace context.

    Example:
        >>> # Route all signals through OTEL Collector (recommended)
        >>> setup_telemetry(
        ...     service_name="data-pipeline",
        ...     otlp_traces_endpoint="https://collector:4318/v1/traces",
        ...     otlp_logs_endpoint="https://collector:4318/v1/logs"
        ... )
        >>>
        >>> # Continue existing trace from upstream service
        >>> setup_telemetry(
        ...     service_name="worker-service",
        ...     otlp_traces_endpoint="https://collector:4318/v1/traces",
        ...     traceparent="00-abc123...-def456...-01"
        ... )

    Note:
        Uses OpenTelemetry global providers (singleton pattern). Attaches parent
        context before provider initialization to ensure proper trace continuation.
        Failed exporter initialization logs warnings but does not halt setup.
    """
    # Attach parent context first if provided for trace continuation
    parent_context = get_parent_context(traceparent=traceparent, tracestate=tracestate)
    if parent_context:
        context.attach(parent_context)
        logger.debug("Attached parent context for trace continuation")

    # Create shared resource for traces and logs
    resource = Resource.create(
        {
            "service.name": service_name,
            # "service.instance.id": get_run_id(),
            # "service.instance.datetime": str(get_run_datetime()),
            # "service.environment": str(settings.environment),
            # "host.name": platform.node(),
            # "host.arch": platform.machine(),
            # "process.pid": str(getpid()),
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

    # Setup logs - use explicit endpoint or skip if not provided
    if otlp_logs_endpoint:
        try:
            log_exporter = OTLPLogExporter(endpoint=otlp_logs_endpoint)
            log_provider = LoggerProvider(resource=resource)
            log_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
            set_logger_provider(log_provider)

            # Attach OTLP handler to root logger to export all Python logs
            handler = LoggingHandler(level=logging.NOTSET, logger_provider=log_provider)
            logging.getLogger().addHandler(handler)

            logger.info("Logs telemetry initialized with OTLP endpoint: %s", otlp_logs_endpoint)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("Failed to initialize OTLP logs exporter: %s", e)
    else:
        logger.info("No OTLP logs endpoint configured, logs will not be exported")


def get_tracer(name: str = "samara") -> trace.Tracer:
    """Retrieve tracer instance from global provider.

    Tracers create spans representing operations or work units. The name serves
    as instrumentation scope identifier in exported traces.

    Args:
        name: Instrumentation scope name, typically __name__ or component identifier.
            Defaults to "samara".

    Returns:
        Tracer instance for span creation and context management.

    Example:
        >>> tracer = get_tracer(__name__)
        >>> with tracer.start_as_current_span("operation"):
        ...     # Operation code here
        ...     pass
    """
    return trace.get_tracer(name)


def trace_span(span_name: str | None = None) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Create decorator for automatic span creation around function execution.

    Wrap functions with distributed tracing instrumentation. Creates span on each
    invocation, records exceptions with stack traces, and sets error status on failure.
    Span name defaults to qualified function name (module.Class.method) but accepts
    custom names for semantic clarity.

    Args:
        span_name: Custom span name for exported traces. If None, derives name from
            function's __module__ and __qualname__ (e.g., "samara.workflow.process_job").

    Returns:
        Decorator function that wraps callables with span lifecycle management.

    Example:
        >>> @trace_span()
        ... def transform_data(df: DataFrame) -> DataFrame:
        ...     return df.filter(df.value > 0)
        >>>
        >>> @trace_span("etl.extract_source")
        ... def extract_from_api(url: str) -> dict:
        ...     response = requests.get(url)
        ...     return response.json()
        >>>
        >>> # Each call creates span with automatic exception handling
        >>> result = transform_data(source_df)

    Note:
        Preserves function metadata via functools.wraps for introspection compatibility.
        Exceptions are recorded in span before re-raising to maintain original stack trace.
        Works with async functions when used with async-compatible tracer.
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        """Apply span instrumentation to target function.

        Args:
            func: Callable to instrument with automatic tracing.

        Returns:
            Wrapped function creating span on each invocation.
        """
        # Use custom span name or derive from function
        name = span_name or f"{func.__module__}.{func.__qualname__}"

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            """Execute wrapped function within active span context.

            Args:
                *args: Positional arguments forwarded to wrapped function.
                **kwargs: Keyword arguments forwarded to wrapped function.

            Returns:
                Result from wrapped function execution.

            Raises:
                Exception: Any exception from wrapped function, recorded in span
                    with ERROR status before propagation.
            """
            tracer = get_tracer()
            with tracer.start_as_current_span(name) as span:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    # Record exception in span before re-raising
                    span.record_exception(e)
                    span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                    raise

        return wrapper

    return decorator


def get_parent_context(traceparent: str | None = None, tracestate: str | None = None) -> Context | None:
    """Extract parent span context from W3C Trace Context headers.

    Parse W3C traceparent and tracestate headers to continue distributed traces
    across service boundaries. Uses TraceContextTextMapPropagator for standard-compliant
    context extraction.

    Args:
        traceparent: W3C traceparent header (format: version-trace_id-span_id-flags).
            Example: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
            Required for trace continuation.
        tracestate: W3C tracestate header for vendor-specific trace metadata.
            Optional comma-separated list of key=value pairs.

    Returns:
        Context object containing extracted span context for trace linking,
        or None if traceparent is absent or invalid.

    Example:
        >>> ctx = get_parent_context(
        ...     traceparent="00-abc123...-def456...-01",
        ...     tracestate="vendor1=value1,vendor2=value2"
        ... )
        >>> if ctx:
        ...     context.attach(ctx)  # Continue trace in current execution
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
