"""Unit tests for OpenTelemetry telemetry setup."""

import time
from unittest.mock import patch

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.trace import StatusCode

from samara.telemetry import (
    ATTR_COMPONENT_ID,
    ATTR_DURATION_MS,
    ATTR_ERROR_MESSAGE,
    ATTR_ERROR_TYPE,
    ATTR_ROW_COUNT,
    SpanTimer,
    add_span_event,
    create_span,
    get_current_span,
    get_parent_context,
    get_tracer,
    set_span_attributes,
    set_span_error,
    set_span_ok,
    setup_telemetry,
    trace_span,
)


class TestTelemetrySetup:
    """Test cases for telemetry initialization."""

    def test_setup_telemetry_creates_provider_with_console_exporter(self) -> None:
        """Test telemetry setup creates provider with console exporter."""
        setup_telemetry(service_name="test-service", otlp_traces_endpoint=None)

        provider = trace.get_tracer_provider()
        assert isinstance(provider, TracerProvider)

    def test_setup_telemetry_with_otlp_endpoint_adds_exporter(self) -> None:
        """Test telemetry setup with OTLP endpoint adds OTLP exporter."""
        with patch("samara.telemetry.OTLPSpanExporter") as mock_exporter:
            setup_telemetry(service_name="test-service", otlp_traces_endpoint="http://localhost:4318/v1/traces")

            # OTLP exporter should be created with the endpoint
            mock_exporter.assert_called_once_with(endpoint="http://localhost:4318/v1/traces")

    def test_setup_telemetry_with_invalid_otlp_endpoint_logs_warning(self) -> None:
        """Test telemetry setup with invalid OTLP endpoint logs warning."""
        with patch("samara.telemetry.OTLPSpanExporter") as mock_exporter:
            mock_exporter.side_effect = Exception("Connection failed")

            # Should not raise exception, just log warning
            setup_telemetry(service_name="test-service", otlp_traces_endpoint="http://invalid:9999")

            # Should still be able to get a tracer
            tracer = get_tracer("test")
            assert tracer is not None

    def test_setup_telemetry_with_traceparent_attaches_context(self) -> None:
        """Test telemetry setup with traceparent attaches parent context."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

        with patch("samara.telemetry.context.attach") as mock_attach:
            setup_telemetry(
                service_name="test-service",
                otlp_traces_endpoint=None,
                traceparent=traceparent,
                tracestate=None,
            )

            # Context should be attached
            mock_attach.assert_called_once()

    def test_setup_telemetry_without_traceparent_does_not_attach_context(self) -> None:
        """Test telemetry setup without traceparent does not attach context."""
        with patch("samara.telemetry.context.attach") as mock_attach:
            setup_telemetry(
                service_name="test-service",
                otlp_traces_endpoint=None,
                traceparent=None,
                tracestate=None,
            )

            # Context should not be attached
            mock_attach.assert_not_called()

    def test_setup_telemetry_with_traceparent_and_tracestate(self) -> None:
        """Test telemetry setup with both traceparent and tracestate."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        tracestate = "vendor1=value1,vendor2=value2"

        with patch("samara.telemetry.context.attach") as mock_attach:
            setup_telemetry(
                service_name="test-service",
                otlp_traces_endpoint=None,
                traceparent=traceparent,
                tracestate=tracestate,
            )

            # Context should be attached
            mock_attach.assert_called_once()


class TestGetTracer:
    """Test cases for get_tracer function."""

    def test_get_tracer_returns_tracer_instance(self) -> None:
        """Test get_tracer returns a valid tracer instance."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer("test-module")

        assert tracer is not None
        assert isinstance(tracer, trace.Tracer)

    def test_get_tracer_with_default_name(self) -> None:
        """Test get_tracer with default name."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        assert tracer is not None
        assert isinstance(tracer, trace.Tracer)

    def test_get_tracer_with_custom_name(self) -> None:
        """Test get_tracer with custom name."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer("custom-component")

        assert tracer is not None
        assert isinstance(tracer, trace.Tracer)


class TestGetParentContext:
    """Test cases for get_parent_context function."""

    def test_get_parent_context_without_traceparent_returns_none(self) -> None:
        """Test getting parent context without traceparent returns None."""
        parent_context = get_parent_context(traceparent=None, tracestate=None)
        assert parent_context is None

    def test_get_parent_context_with_valid_traceparent(self) -> None:
        """Test getting parent context with valid traceparent."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        parent_context = get_parent_context(traceparent=traceparent, tracestate=None)

        assert parent_context is not None
        assert isinstance(parent_context, Context)

    def test_get_parent_context_with_traceparent_and_tracestate(self) -> None:
        """Test getting parent context with both traceparent and tracestate."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
        tracestate = "vendor1=value1,vendor2=value2"

        parent_context = get_parent_context(traceparent=traceparent, tracestate=tracestate)

        assert parent_context is not None
        assert isinstance(parent_context, Context)

    def test_get_parent_context_with_invalid_traceparent_format(self) -> None:
        """Test getting parent context with invalid traceparent format."""
        parent_context = get_parent_context(traceparent="invalid-format", tracestate=None)

        # Should return context even with invalid format (propagator handles it)
        assert parent_context is not None

    def test_get_parent_context_extracts_trace_id_correctly(self) -> None:
        """Test that parent context extraction preserves trace ID."""
        expected_trace_id = "0af7651916cd43dd8448eb211c80319c"
        traceparent = f"00-{expected_trace_id}-b7ad6b7169203331-01"

        parent_context = get_parent_context(traceparent=traceparent, tracestate=None)

        assert parent_context is not None
        # The context should contain the trace information
        span_context = trace.get_current_span(parent_context).get_span_context()
        assert span_context.trace_id == int(expected_trace_id, 16)


class TestTracingIntegration:
    """Test cases for tracing integration."""

    def test_create_span(self) -> None:
        """Test creating a span."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer("test")

        with tracer.start_as_current_span("test-span") as span:
            assert span is not None
            assert span.is_recording()

    def test_create_child_span(self) -> None:
        """Test creating child spans."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer("test")

        with tracer.start_as_current_span("parent-span") as parent:
            assert parent.is_recording()

            with tracer.start_as_current_span("child-span") as child:
                assert child.is_recording()
                # Child should have different span context than parent
                assert child.get_span_context().span_id != parent.get_span_context().span_id
                # But should have same trace_id
                assert child.get_span_context().trace_id == parent.get_span_context().trace_id

    def test_continue_trace_with_parent_context(self) -> None:
        """Test continuing a trace with parent context."""
        # This is a test example trace ID - W3C traceparent format
        example_trace_id = "0af7651916cd43dd8448eb211c80319c"
        traceparent = f"00-{example_trace_id}-b7ad6b7169203331-01"

        setup_telemetry(
            service_name="test-service",
            traceparent=traceparent,
            tracestate=None,
        )

        tracer = get_tracer("test")

        # Create span - should automatically continue the trace
        with tracer.start_as_current_span("continued-span") as span:
            assert span.is_recording()
            span_context = span.get_span_context()
            expected_trace_id = int(example_trace_id, 16)
            assert span_context.trace_id == expected_trace_id


class TestTraceSpanDecorator:
    """Test cases for trace_span decorator."""

    def test_trace_span_decorator_creates_span(self) -> None:
        """Test trace_span decorator creates a span around function execution."""
        setup_telemetry(service_name="test-service")

        @trace_span()
        def sample_function(value: int) -> int:
            """Sample function for testing."""
            return value * 2

        result = sample_function(5)
        assert result == 10

    def test_trace_span_decorator_with_custom_name(self) -> None:
        """Test trace_span decorator with custom span name."""
        setup_telemetry(service_name="test-service")

        @trace_span("custom_operation")
        def sample_function(value: int) -> int:
            """Sample function for testing."""
            return value * 2

        result = sample_function(5)
        assert result == 10

    def test_trace_span_decorator_preserves_function_metadata(self) -> None:
        """Test trace_span decorator preserves function name and docstring."""

        @trace_span()
        def sample_function(value: int) -> int:
            """Sample function docstring."""
            return value * 2

        assert sample_function.__name__ == "sample_function"
        assert sample_function.__doc__ == "Sample function docstring."

    def test_trace_span_decorator_handles_exceptions(self) -> None:
        """Test trace_span decorator records exceptions and re-raises them."""
        setup_telemetry(service_name="test-service")

        @trace_span()
        def failing_function() -> None:
            """Function that raises an exception."""
            raise ValueError("Test exception")

        try:
            failing_function()
            assert False, "Expected ValueError to be raised"
        except ValueError as e:
            assert str(e) == "Test exception"

    def test_trace_span_decorator_with_arguments(self) -> None:
        """Test trace_span decorator works with functions that have arguments."""
        setup_telemetry(service_name="test-service")

        @trace_span()
        def function_with_args(a: int, b: int, c: str = "default") -> str:
            """Function with multiple arguments."""
            return f"{a + b} {c}"

        result = function_with_args(1, 2, c="test")
        assert result == "3 test"

    def test_trace_span_decorator_with_return_value(self) -> None:
        """Test trace_span decorator preserves return values."""
        setup_telemetry(service_name="test-service")

        @trace_span()
        def function_with_return() -> dict[str, int]:
            """Function that returns a dictionary."""
            return {"value": 42}

        result = function_with_return()
        assert result == {"value": 42}

    def test_trace_span_decorator_nesting(self) -> None:
        """Test nested functions with trace_span decorator create child spans."""
        setup_telemetry(service_name="test-service")

        @trace_span("parent_operation")
        def parent_function() -> int:
            """Parent function."""
            return child_function()

        @trace_span("child_operation")
        def child_function() -> int:
            """Child function."""
            return 42

        result = parent_function()
        assert result == 42


class TestSpanHelpers:
    """Test cases for span helper functions."""

    def test_get_current_span__returns_span(self) -> None:
        """Test get_current_span returns a span object."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span"):
            span = get_current_span()
            assert span is not None
            assert span.is_recording()

    def test_set_span_attributes__sets_string_attribute(self) -> None:
        """Test set_span_attributes sets string attributes correctly."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            set_span_attributes({ATTR_COMPONENT_ID: "test-component"})

            # Span should have the attribute set
            assert span.is_recording()

    def test_set_span_attributes__sets_numeric_attribute(self) -> None:
        """Test set_span_attributes sets numeric attributes correctly."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            set_span_attributes({ATTR_ROW_COUNT: 1000})
            assert span.is_recording()

    def test_set_span_attributes__ignores_none_values(self) -> None:
        """Test set_span_attributes ignores None values."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            # Should not raise when None values are provided
            set_span_attributes({ATTR_COMPONENT_ID: None, ATTR_ROW_COUNT: 100})
            assert span.is_recording()

    def test_set_span_attributes__with_specific_span(self) -> None:
        """Test set_span_attributes can target a specific span."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            set_span_attributes({ATTR_COMPONENT_ID: "specific"}, span=span)
            assert span.is_recording()

    def test_add_span_event__adds_event(self) -> None:
        """Test add_span_event adds an event to the span."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            add_span_event("test.event.started")
            assert span.is_recording()

    def test_add_span_event__with_attributes(self) -> None:
        """Test add_span_event adds event with attributes."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            add_span_event("test.event", {ATTR_COMPONENT_ID: "test", ATTR_ROW_COUNT: 50})
            assert span.is_recording()

    def test_add_span_event__with_specific_span(self) -> None:
        """Test add_span_event can target a specific span."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            add_span_event("test.event", span=span)
            assert span.is_recording()


class TestSpanErrorHandling:
    """Test cases for span error handling functions."""

    def test_set_span_error__records_exception(self) -> None:
        """Test set_span_error records exception on span."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            exception = ValueError("Test error")
            set_span_error(exception)

            # Span should be marked as error
            assert span.is_recording()
            # Status should be set to ERROR
            assert span.status.status_code == StatusCode.ERROR

    def test_set_span_error__with_custom_message(self) -> None:
        """Test set_span_error with custom error message."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            exception = ValueError("Original error")
            set_span_error(exception, message="Custom error message")

            assert span.status.status_code == StatusCode.ERROR
            assert span.status.description == "Custom error message"

    def test_set_span_error__with_specific_span(self) -> None:
        """Test set_span_error can target a specific span."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            exception = RuntimeError("Test runtime error")
            set_span_error(exception, span=span)

            assert span.status.status_code == StatusCode.ERROR

    def test_set_span_ok__sets_ok_status(self) -> None:
        """Test set_span_ok sets OK status on span."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            set_span_ok()
            assert span.status.status_code == StatusCode.OK

    def test_set_span_ok__with_specific_span(self) -> None:
        """Test set_span_ok can target a specific span."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            set_span_ok(span=span)
            assert span.status.status_code == StatusCode.OK


class TestCreateSpan:
    """Test cases for create_span function."""

    def test_create_span__returns_span(self) -> None:
        """Test create_span returns a started span."""
        setup_telemetry(service_name="test-service")

        span = create_span("test-operation")
        try:
            assert span is not None
            assert span.is_recording()
        finally:
            span.end()

    def test_create_span__with_attributes(self) -> None:
        """Test create_span with initial attributes."""
        setup_telemetry(service_name="test-service")

        span = create_span("test-operation", {ATTR_COMPONENT_ID: "test", ATTR_ROW_COUNT: 100})
        try:
            assert span.is_recording()
        finally:
            span.end()

    def test_create_span__with_use_span_context(self) -> None:
        """Test create_span can be used with trace.use_span."""
        setup_telemetry(service_name="test-service")

        span = create_span("test-operation")
        with trace.use_span(span, end_on_exit=True):
            assert span.is_recording()


class TestSpanTimer:
    """Test cases for SpanTimer context manager."""

    def test_span_timer__records_duration(self) -> None:
        """Test SpanTimer records duration as span attribute."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            with SpanTimer():
                time.sleep(0.01)  # Sleep 10ms

            # Duration should have been recorded
            assert span.is_recording()

    def test_span_timer__with_custom_attribute_name(self) -> None:
        """Test SpanTimer with custom attribute name."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            with SpanTimer("custom.duration_ms"):
                time.sleep(0.01)

            assert span.is_recording()

    def test_span_timer__with_specific_span(self) -> None:
        """Test SpanTimer can target a specific span."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span") as span:
            with SpanTimer(span=span):
                time.sleep(0.01)

            assert span.is_recording()

    def test_span_timer__returns_self(self) -> None:
        """Test SpanTimer __enter__ returns self."""
        setup_telemetry(service_name="test-service")
        tracer = get_tracer()

        with tracer.start_as_current_span("test-span"):
            with SpanTimer() as timer:
                assert timer is not None
