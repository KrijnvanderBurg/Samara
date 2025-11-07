# Telemetry Setup

## Overview

Samara includes built-in OpenTelemetry support for distributed tracing and metrics collection. This enables you to monitor your data pipelines and track their performance in production environments.

## Architecture

The telemetry system consists of two main components:

1. **Distributed Tracing**: Tracks the execution flow of your pipelines across services
2. **Metrics Collection**: Captures quantitative measurements like invocation counts and durations

## Configuration

### Environment Variables

You can configure telemetry endpoints using environment variables:

```bash
export SAMARA_OTLP_TRACES_ENDPOINT="http://otel-collector:4318/v1/traces"
export SAMARA_OTLP_METRICS_ENDPOINT="http://otel-collector:4318/v1/metrics"
export SAMARA_TRACE_PARENT="00-trace-id-span-id-01"  # Optional: for trace continuation
export SAMARA_TRACE_STATE="key=value"  # Optional: for trace state
```

### CLI Options

Alternatively, pass telemetry configuration via CLI options:

```bash
samara run \
  --workflow-filepath job.json \
  --alert-filepath alert.json \
  --otlp-traces-endpoint "http://otel-collector:4318/v1/traces" \
  --otlp-metrics-endpoint "http://otel-collector:4318/v1/metrics"
```

## Metrics

### Available Metrics

Samara currently exports the following metrics:

- **`samara.cli.run.invocations`**: Counter tracking the number of times the `run` command is invoked
  - Type: Counter
  - Unit: `1` (count)
  - Attributes: `command="run"`

### Adding Custom Metrics

To add custom metrics in your code:

```python
from samara.telemetry import get_meter

meter = get_meter("my_component")

# Create a counter
counter = meter.create_counter(
    name="my.custom.counter",
    description="Description of what this counts",
    unit="1",
)
counter.add(1, {"attribute_key": "attribute_value"})

# Create a histogram
histogram = meter.create_histogram(
    name="my.custom.duration",
    description="Duration of some operation",
    unit="ms",
)
histogram.record(123.45, {"operation": "data_load"})
```

## Distributed Tracing

### Trace Continuation

Samara supports W3C Trace Context for continuing traces across service boundaries:

```bash
# Parent service creates a trace and passes headers downstream
samara run \
  --workflow-filepath job.json \
  --alert-filepath alert.json \
  --trace-parent "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01" \
  --trace-state "congo=t61rcWkgMzE"
```

### Creating Custom Spans

To add custom tracing in your code:

```python
from samara.telemetry import get_tracer

tracer = get_tracer("my_component")

with tracer.start_as_current_span("my_operation"):
    # Your code here
    pass
```

## Backend Setup

### Prometheus Setup

Prometheus v3.0+ supports OTLP write receiver as an experimental feature:

```yaml
# docker-compose.yml
services:
  prometheus:
    image: prom/prometheus:v3.0.1
    ports:
      - "9090:9090"
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--enable-feature=otlp-write-receiver'
```

The OTLP endpoint for Prometheus is:
```
http://<prometheus-host>:9090/api/v1/otlp/v1/metrics
```

### OTEL Collector Setup

OpenTelemetry Collector provides a vendor-agnostic way to receive, process, and export telemetry:

```yaml
# docker-compose.yml
services:
  otel-collector:
    image: otel/opentelemetry-collector-contrib:latest
    ports:
      - "4318:4318"    # OTLP HTTP receiver
    volumes:
      - ./otel-config.yaml:/etc/otel/config.yaml
    command: ["--config=/etc/otel/config.yaml"]
```

### Jaeger Setup

Jaeger provides distributed tracing capabilities:

```yaml
# docker-compose.yml
services:
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"  # UI
      - "4318:4318"    # OTLP HTTP receiver
    environment:
      - COLLECTOR_OTLP_ENABLED=true
```

The OTLP endpoint for Jaeger is:
```
http://<jaeger-host>:4318/v1/traces
```

## Accessing Telemetry Data

### Prometheus UI

Access Prometheus at `http://localhost:9090` to:
- Query metrics using PromQL
- View metric values over time
- Create custom dashboards

Example query to see Samara invocations:
```promql
samara_cli_run_invocations_total
```

### Jaeger UI

Access Jaeger at `http://localhost:16686` to:
- Search for traces by service, operation, or tags
- View trace timelines and span details
- Analyze service dependencies

### Grafana Integration

Connect Grafana to Prometheus and Jaeger for unified dashboards:

1. Add Prometheus as a data source
2. Add Jaeger as a data source
3. Create dashboards combining metrics and traces

## Best Practices

1. **Use meaningful metric names**: Follow the format `component.subsystem.metric_name`
2. **Add relevant attributes**: Include context like operation type, status, etc.
3. **Keep cardinality low**: Avoid high-cardinality attributes (e.g., user IDs, request IDs)
4. **Create spans for key operations**: Trace important steps in your pipeline
5. **Continue traces across services**: Pass trace context when calling other services

## Troubleshooting

### Metrics not appearing in Prometheus

1. Check that Prometheus has `--enable-feature=otlp-write-receiver` enabled
2. Verify the endpoint URL: `http://<host>:9090/api/v1/otlp/v1/metrics`
3. Check Samara logs for export errors
4. Wait 60 seconds (default export interval) for metrics to appear
5. Check Prometheus logs for any ingestion errors

### Traces not appearing in Jaeger

1. Check that Jaeger has `COLLECTOR_OTLP_ENABLED=true` set
2. Verify the endpoint URL: `http://<host>:4318/v1/traces`
3. Check Samara logs for export errors
4. Ensure traces are being created (check console output)

### Connection Refused Errors

If you see connection errors, ensure:
- The telemetry backends are running
- Network connectivity exists between Samara and the backends
- DNS resolution works (for Docker networks)
- Firewall rules allow the connections
