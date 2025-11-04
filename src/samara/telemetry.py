"""Basic OpenTelemetry tracing setup."""

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.semconv.resource import ResourceAttributes


def init_telemetry() -> None:
    """Initialize OpenTelemetry tracing."""
    resource = Resource(
        attributes={
            ResourceAttributes.SERVICE_NAME: "samara",
        }
    )

    provider = TracerProvider(resource=resource)
    processor = BatchSpanProcessor(ConsoleSpanExporter())
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)


# Get tracer
tracer = trace.get_tracer("samara")
