"""OpenTelemetry telemetry setup for distributed tracing and logs.

This module provides comprehensive OpenTelemetry instrumentation for:
1. Continuing existing traces via W3C trace context
2. Exporting traces to any OTLP-compatible backend (OTEL Collector, Jaeger, etc.)
3. Exporting logs to any OTLP-compatible backend (OTEL Collector, Loki, etc.)
4. Rich span attributes and events for deep observability into pipeline execution

The design supports flexible backend configuration - use OTEL Collector as a central
aggregation point, or send directly to specific backends. Each signal (traces, logs)
can be configured independently for maximum deployment flexibility.
"""

import functools
import logging
import time
from collections.abc import Callable
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
from opentelemetry.trace import Span, SpanKind, StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from samara import get_run_id
from samara.settings import AppSettings, get_settings
from samara.utils.logger import get_logger

logger: logging.Logger = get_logger(__name__)
settings: AppSettings = get_settings()

# Type variables for generic decorator support
P = ParamSpec("P")
T = TypeVar("T")

# Semantic attribute names for workflow telemetry
# Following OpenTelemetry semantic conventions where applicable
ATTR_WORKFLOW_ID = "samara.workflow.id"
ATTR_WORKFLOW_ENABLED = "samara.workflow.enabled"
ATTR_JOB_ID = "samara.job.id"
ATTR_JOB_ENGINE = "samara.job.engine"
ATTR_JOB_ENABLED = "samara.job.enabled"
ATTR_JOB_EXTRACT_COUNT = "samara.job.extract_count"
ATTR_JOB_TRANSFORM_COUNT = "samara.job.transform_count"
ATTR_JOB_LOAD_COUNT = "samara.job.load_count"
ATTR_PHASE_NAME = "samara.phase.name"
ATTR_PHASE_COMPONENT_INDEX = "samara.phase.component_index"
ATTR_PHASE_COMPONENT_TOTAL = "samara.phase.component_total"
ATTR_COMPONENT_ID = "samara.component.id"
ATTR_COMPONENT_TYPE = "samara.component.type"
ATTR_DATA_FORMAT = "samara.data.format"
ATTR_DATA_LOCATION = "samara.data.location"
ATTR_DATA_METHOD = "samara.data.method"
ATTR_ROW_COUNT = "samara.data.row_count"
ATTR_ROW_COUNT_BEFORE = "samara.data.row_count_before"
ATTR_ROW_COUNT_AFTER = "samara.data.row_count_after"
ATTR_COLUMN_COUNT = "samara.data.column_count"
ATTR_TRANSFORM_FUNCTION_TYPE = "samara.transform.function_type"
ATTR_TRANSFORM_FUNCTION_INDEX = "samara.transform.function_index"
ATTR_TRANSFORM_FUNCTION_TOTAL = "samara.transform.function_total"
ATTR_DURATION_MS = "samara.duration_ms"
ATTR_ERROR_TYPE = "samara.error.type"
ATTR_ERROR_MESSAGE = "samara.error.message"


def setup_telemetry(
    service_name: str,
    otlp_traces_endpoint: str | None = None,
    otlp_logs_endpoint: str | None = None,
    traceparent: str | None = None,
    tracestate: str | None = None,
) -> None:
    """Initialize OpenTelemetry with flexible OTLP exporters for traces and logs.

    Sets up a basic telemetry configuration with:
    - Service name identification
    - OTLP HTTP exporter for traces (push-based) - configurable backend
    - OTLP HTTP exporter for logs (push-based) - configurable backend
    - Batch span processor for efficient trace export
    - Logging handler bridge for Python logging to OTLP
    - Parent context attachment for trace continuation

    Supports flexible backend configuration:
    - Send all signals to OTEL Collector (recommended):
      traces: "https://otel-collector:4318/v1/traces"
      logs: "https://otel-collector:4318/v1/logs"
    - Send directly to specific backends:
      traces: "https://jaeger:4318/v1/traces"
      logs: "https://loki:3100/loki/api/v1/push"
    - Mix and match as needed for your deployment architecture

    Args:
        service_name: Name of the service for trace identification
        otlp_traces_endpoint: OTLP endpoint URL for traces. Can be OTEL Collector,
            Jaeger, or any OTLP-compatible backend (e.g., "https://localhost:4318/v1/traces").
            If None, telemetry is configured but traces won't be exported.
        otlp_logs_endpoint: OTLP endpoint URL for logs. Can be OTEL Collector,
            Loki, or any OTLP-compatible backend (e.g., "https://localhost:4318/v1/logs").
            If None, logs won't be exported.
        traceparent: W3C traceparent header for continuing existing trace
        tracestate: W3C tracestate header for continuing existing trace

    Note:
        This function is idempotent - calling it multiple times will only
        initialize once. Uses OpenTelemetry's global tracer and logger providers.
        The flexible endpoint configuration allows you to adapt to different
        deployment scenarios: local development, production with OTEL Collector,
        or direct backend integration.
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
            "service.instance.id": get_run_id(),
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
    """Get a tracer instance for creating spans.

    Args:
        name: Name of the tracer (typically module or component name)

    Returns:
        Tracer instance for creating spans
    """
    return trace.get_tracer(name)


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


def get_current_span() -> Span:
    """Get the current active span from the trace context.

    Returns:
        The currently active Span from OpenTelemetry context.
    """
    return trace.get_current_span()


def set_span_attributes(attributes: dict[str, Any], span: Span | None = None) -> None:
    """Set multiple attributes on a span.

    Convenience function for setting multiple attributes at once on the
    current span or a specific span. Handles type conversion for common
    Python types and filters out None values.

    Args:
        attributes: Dictionary of attribute key-value pairs. Keys should use
            the semantic attribute constants defined in this module.
        span: Optional span to set attributes on. If None, uses current span.

    Example:
        >>> set_span_attributes({
        ...     ATTR_JOB_ID: "customer_etl",
        ...     ATTR_ROW_COUNT: 1500,
        ...     ATTR_DURATION_MS: 234.5
        ... })
    """
    target_span = span or get_current_span()
    if not target_span.is_recording():
        return

    for key, value in attributes.items():
        if value is not None:
            # Convert to OTEL-compatible types
            if value is True or value is False:
                target_span.set_attribute(key, value)
            else:
                target_span.set_attribute(key, _convert_attribute_value(value))


def add_span_event(
    name: str,
    attributes: dict[str, Any] | None = None,
    span: Span | None = None,
) -> None:
    """Add a timestamped event to a span.

    Events mark significant points in a span's lifecycle, such as starting
    a new phase, completing a step, or noting important state changes.

    Args:
        name: Human-readable name for the event.
        attributes: Optional dictionary of event attributes providing
            additional context about the event.
        span: Optional span to add event to. If None, uses current span.

    Example:
        >>> add_span_event("extract.started", {
        ...     ATTR_COMPONENT_ID: "customers_extract",
        ...     ATTR_DATA_FORMAT: "parquet"
        ... })
    """
    target_span = span or get_current_span()
    if not target_span.is_recording():
        return

    event_attrs: dict[str, Any] = {}
    if attributes:
        for key, value in attributes.items():
            if value is not None:
                event_attrs[key] = _convert_attribute_value(value)

    target_span.add_event(name, attributes=event_attrs if event_attrs else None)


def set_span_error(
    exception: Exception,
    message: str | None = None,
    span: Span | None = None,
) -> None:
    """Mark a span as errored with exception details.

    Records the exception on the span and sets the span status to ERROR.
    This provides comprehensive error tracking in traces.

    Args:
        exception: The exception that occurred.
        message: Optional custom error message. If not provided, uses
            the exception's string representation.
        span: Optional span to set error on. If None, uses current span.
    """
    target_span = span or get_current_span()
    if not target_span.is_recording():
        return

    target_span.record_exception(exception)
    error_message = message or str(exception)
    target_span.set_status(trace.Status(StatusCode.ERROR, error_message))
    target_span.set_attribute(ATTR_ERROR_TYPE, type(exception).__name__)
    target_span.set_attribute(ATTR_ERROR_MESSAGE, error_message)


def set_span_ok(span: Span | None = None) -> None:
    """Mark a span as successfully completed.

    Args:
        span: Optional span to set status on. If None, uses current span.
    """
    target_span = span or get_current_span()
    if target_span.is_recording():
        target_span.set_status(trace.Status(StatusCode.OK))


def create_span(
    name: str,
    attributes: dict[str, Any] | None = None,
    kind: SpanKind = SpanKind.INTERNAL,
) -> Span:
    """Create and start a new span with optional attributes.

    Provides a convenient way to create spans with initial attributes
    for use with context managers or manual span management.

    Args:
        name: Name of the span identifying the operation.
        attributes: Optional initial attributes to set on the span.
        kind: SpanKind indicating the type of span (default: INTERNAL).

    Returns:
        A started Span that should be ended when the operation completes.

    Example:
        >>> span = create_span("process_batch", {ATTR_ROW_COUNT: 100})
        >>> with trace.use_span(span, end_on_exit=True):
        ...     process_data()
    """
    tracer = get_tracer()
    span = tracer.start_span(name, kind=kind)

    if attributes:
        for key, value in attributes.items():
            if value is not None:
                span.set_attribute(key, _convert_attribute_value(value))

    return span


def _convert_attribute_value(value: Any) -> str | int | float | bool:
    """Convert a value to an OTEL-compatible attribute type.

    OpenTelemetry accepts str, int, float, bool for attribute values.
    This function converts other types to strings.

    Args:
        value: Value to convert.

    Returns:
        Converted value safe for OTEL attributes.
    """
    if value is None:
        return ""
    if value is True:
        return True
    if value is False:
        return False
    if type(value) in (int, float, str):
        return value
    return str(value)


class SpanTimer:
    """Context manager for timing operations and recording duration.

    Measures elapsed time for a code block and records it as a span
    attribute. Useful for capturing detailed timing information within
    a parent span.

    Attributes:
        attribute_name: The attribute key for storing duration.
        span: The span to record duration on.

    Example:
        >>> with SpanTimer(ATTR_DURATION_MS):
        ...     perform_expensive_operation()
    """

    def __init__(
        self,
        attribute_name: str = ATTR_DURATION_MS,
        span: Span | None = None,
    ) -> None:
        """Initialize the timer.

        Args:
            attribute_name: Attribute key for storing duration in milliseconds.
            span: Optional span to record on. If None, uses current span.
        """
        self.attribute_name = attribute_name
        self.span = span
        self._start_time: float = 0.0

    def __enter__(self) -> "SpanTimer":
        """Start timing."""
        self._start_time = time.perf_counter()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop timing and record duration."""
        duration_ms = (time.perf_counter() - self._start_time) * 1000
        target_span = self.span or get_current_span()
        if target_span.is_recording():
            target_span.set_attribute(self.attribute_name, duration_ms)
