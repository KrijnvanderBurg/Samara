"""Unit tests for OpenTelemetry telemetry setup."""

from unittest.mock import patch

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace import TracerProvider

from samara.telemetry import get_parent_context, get_tracer, setup_telemetry


class TestTelemetrySetup:
    """Test cases for telemetry initialization."""

    def test_setup_telemetry_creates_provider_with_console_exporter(self) -> None:
        """Test telemetry setup creates provider with console exporter."""
        setup_telemetry(service_name="test-service", otlp_endpoint=None)

        provider = trace.get_tracer_provider()
        assert isinstance(provider, TracerProvider)

    def test_setup_telemetry_with_otlp_endpoint_adds_exporter(self) -> None:
        """Test telemetry setup with OTLP endpoint adds OTLP exporter."""
        with patch("samara.telemetry.OTLPSpanExporter") as mock_exporter:
            setup_telemetry(service_name="test-service", otlp_endpoint="http://localhost:4318")

            # OTLP exporter should be created with the endpoint
            mock_exporter.assert_called_once_with(endpoint="http://localhost:4318", insecure=True)

    def test_setup_telemetry_with_invalid_otlp_endpoint_logs_warning(self) -> None:
        """Test telemetry setup with invalid OTLP endpoint logs warning."""
        with patch("samara.telemetry.OTLPSpanExporter") as mock_exporter:
            mock_exporter.side_effect = Exception("Connection failed")

            # Should not raise exception, just log warning
            setup_telemetry(service_name="test-service", otlp_endpoint="http://invalid:9999")

            # Should still be able to get a tracer
            tracer = get_tracer("test")
            assert tracer is not None

    def test_setup_telemetry_with_traceparent_attaches_context(self) -> None:
        """Test telemetry setup with traceparent attaches parent context."""
        traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"

        with patch("samara.telemetry.context.attach") as mock_attach:
            setup_telemetry(
                service_name="test-service",
                otlp_endpoint=None,
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
                otlp_endpoint=None,
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
                otlp_endpoint=None,
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
