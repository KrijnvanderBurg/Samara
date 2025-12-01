"""OpenTelemetry telemetry setup for distributed tracing and metrics.

This module provides a simple OpenTelemetry configuration for:
1. Continuing existing traces via W3C trace context
2. Exporting traces to any OTLP-compatible backend (OTEL Collector, Jaeger, etc.)
3. Collecting and exporting metrics to any OTLP-compatible backend (OTEL Collector, Prometheus, etc.)

The design supports flexible backend configuration - use OTEL Collector as a central
aggregation point, or send directly to specific backends. Each signal (traces, metrics)
can be configured independently for maximum deployment flexibility.
"""

import functools
import logging
from collections.abc import Callable

# import platform
# from os import getpid
from typing import Any, ParamSpec, TypeVar

from opentelemetry import context, metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# from samara import get_run_datetime, get_run_id
from samara.settings import AppSettings, get_settings
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)
settings: AppSettings = get_settings()

# Type variables for generic decorator support
P = ParamSpec("P")
T = TypeVar("T")


def setup_telemetry(
    service_name: str,
    otlp_traces_endpoint: str | None = None,
    otlp_metrics_endpoint: str | None = None,
    otlp_logs_endpoint: str | None = None,
    traceparent: str | None = None,
    tracestate: str | None = None,
) -> None:
    """Initialize OpenTelemetry with flexible OTLP exporters for traces, metrics, and logs.

    Sets up a basic telemetry configuration with:
    - Service name identification
    - OTLP HTTP exporter for traces (push-based) - configurable backend
    - OTLP HTTP exporter for metrics (push-based) - configurable backend
    - OTLP HTTP exporter for logs (push-based) - configurable backend
    - Batch span processor for efficient trace export
    - Periodic metric reader for regular metric export
    - Logging handler bridge for Python logging to OTLP
    - Parent context attachment for trace continuation

    Supports flexible backend configuration:
    - Send all signals to OTEL Collector (recommended):
      traces: "https://otel-collector:4318/v1/traces"
      metrics: "https://otel-collector:4318/v1/metrics"
      logs: "https://otel-collector:4318/v1/logs"
    - Send directly to specific backends:
      traces: "https://jaeger:4318/v1/traces"
      metrics: "https://prometheus:9090/api/v1/otlp/v1/metrics"
      logs: "https://loki:3100/loki/api/v1/push"
    - Mix and match as needed for your deployment architecture

    Args:
        service_name: Name of the service for trace identification
        otlp_traces_endpoint: OTLP endpoint URL for traces. Can be OTEL Collector,
            Jaeger, or any OTLP-compatible backend (e.g., "https://localhost:4318/v1/traces").
            If None, telemetry is configured but traces won't be exported.
        otlp_metrics_endpoint: OTLP endpoint URL for metrics. Can be OTEL Collector,
            Prometheus, or any OTLP-compatible backend (e.g., "https://localhost:4318/v1/metrics").
            If None, metrics won't be exported.
        otlp_logs_endpoint: OTLP endpoint URL for logs. Can be OTEL Collector,
            Loki, or any OTLP-compatible backend (e.g., "https://localhost:4318/v1/logs").
            If None, logs won't be exported.
        traceparent: W3C traceparent header for continuing existing trace
        tracestate: W3C tracestate header for continuing existing trace

    Note:
        This function is idempotent - calling it multiple times will only
        initialize once. Uses OpenTelemetry's global tracer, meter, and logger providers.
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

    # Setup metrics - use explicit endpoint or skip if not provided
    if otlp_metrics_endpoint:
        try:
            metric_exporter = OTLPMetricExporter(endpoint=otlp_metrics_endpoint)
            metric_reader = PeriodicExportingMetricReader(metric_exporter, export_interval_millis=2000)
            meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
            metrics.set_meter_provider(meter_provider)
            logger.info("Metrics telemetry initialized with OTLP endpoint: %s", otlp_metrics_endpoint)
        except Exception as e:  # pylint: disable=broad-except
            logger.warning("Failed to initialize OTLP metrics exporter: %s", e)
    else:
        logger.info("No OTLP metrics endpoint configured, metrics will not be exported")

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


def trace_span(span_name: str | None = None) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorate a function to automatically create an OpenTelemetry span.

    This decorator simplifies tracing by automatically creating a span around
    the decorated function. The span name defaults to the function name but
    can be overridden with a custom name. Any exceptions raised within the
    function are automatically recorded in the span.

    Args:
        span_name: Optional custom name for the span. If None, uses the
            function's qualified name (module.function).

    Returns:
        A decorator that wraps the function with automatic span creation.

    Example:
        >>> @trace_span()
        ... def process_data(data: list[int]) -> int:
        ...     return sum(data)
        >>>
        >>> @trace_span("custom_operation")
        ... def complex_operation() -> str:
        ...     return "done"
        >>>
        >>> # Spans are created automatically on function calls
        >>> result = process_data([1, 2, 3])

    Note:
        The decorator preserves function metadata (name, docstring, etc.)
        using functools.wraps. Exceptions are recorded in the span with
        full stack traces before being re-raised.
    """

    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        """Wrap function with span creation logic.

        Args:
            func: The function to wrap with tracing.

        Returns:
            The wrapped function with automatic span creation.
        """
        # Use custom span name or derive from function
        name = span_name or f"{func.__module__}.{func.__qualname__}"

        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            """Execute function within a traced span.

            Args:
                *args: Positional arguments passed to the wrapped function.
                **kwargs: Keyword arguments passed to the wrapped function.

            Returns:
                The return value from the wrapped function.

            Raises:
                Any exception raised by the wrapped function after recording
                it in the span.
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
