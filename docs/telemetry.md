# Telemetry

### OpenTelemetry

This repository uses the [OpenTelemetry for Go](https://github.com/open-telemetry/opentelemetry-go) (OTEL) integration.

Telemetry setup is located in the `main.go` [entrypoint](../cmd) of each service.

Dependencies containers have the `Telemetry() telemetry.Telemetry` method.

From the [telemetry.Telemetry](../internal/pkg/telemetry/telemetry.go) interface, you can access the following:

- `Tracer() telemetry.Tracer` for creating spans
- `Meter() telemetry.Meter` for creating metrics.
- `TracerProvider()` and `MeterProvider()` can be used to connect other OTEL integrations.

### DataDog

CLI doesn't display the telemetry yet.

Other services send OpenTelemetry spans and metrics to DataDog.

#### Spans

The official DataDog adapter [ddtrace/opentelemetry](https://pkg.go.dev/github.com/DataDog/dd-trace-go/v2/ddtrace/opentelemetry) is used to
send spans.

Customizations and improvements can be found in [datadog/provider.go](../internal/pkg/telemetry/datadog/provider.go)
and [datadog/tracer.go](../internal/pkg/telemetry/datadog/tracer.go).

#### Metrics

DataDog collects metrics from targets by scraping metrics HTTP endpoint `<host>:9000/metrics`.

The HTTP server with metrics is started by
the [prometheus.ServeMetrics](../internal/pkg/telemetry/metric/prometheus/prometheus.go) function.

## HTTP Server

### Spans

| Span                  | Description                                                                                      |
|-----------------------|--------------------------------------------------------------------------------------------------|
| `http.server.request` | HTTP request. Attributes `keboola.*` contain info about stack, project and token, if applicable. |

## HTTP Client

Client telemetry is implemented in the [keboola-sdk-go](https://github.com/keboola/keboola-sdk-go) repository, in
the [pkg/client/trace/otel](https://github.com/keboola/keboola-sdk-go/tree/main/pkg/client/trace/otel) package.

### Spans

| Span                         | Description                                                                 |
|------------------------------|-----------------------------------------------------------------------------|
| `http.go.api.client.request` | An API operation composed from multiple HTTP requests.                      |
| `http.go.client.request`     | High-level "logical" HTTP request, wraps redirects and retries to one span. |
| `http.request`               | Low-level "physical" HTTP request.                                          |

### Metrics

| Span                                        | Description                                                                                            |
|---------------------------------------------|--------------------------------------------------------------------------------------------------------|
| `keboola_go_client_request_in_flight`       | Number of active HTTP requests.<br>All request redirects and retries are tracked as one record.        |
| `keboola_go_client_request_duration`        | Duration of a high-level HTTP request.<br>All request redirects and retries are tracked as one record. |
| `keboola_go_client_request_parse_in_flight` | Number of active response body parsing operations.                                                     |
| `keboola_go_client_request_parse_duration`  | Duration of HTTP response body parsing.                                                                |
| `keboola_go_http_request_in_flight`         | Number of active HTTP requests. Each redirect and retry is tracked separately.                         |
| `keboola.go.http.request.duration`          | Duration of a low-level HTTP request. Each redirect and retry is tracked separately.                   |
| `keboola.go.http.request.size.count`        | Request content length.                                                                                |
| `keboola.go.http.response.size.count`       | Response content length.                                                                               |

## Background Tasks

### Spans

| Span                     | Description                                                               |
|--------------------------|---------------------------------------------------------------------------|
| `keboola.go.task`        | A background task. The `resource.name` attribute is set to the task type. |


### Metrics

| Span                       | Description                         |
|----------------------------|-------------------------------------|
| `keboola_go_task_running`  | Number of running background tasks. |
| `keboola_go_task_duration` | Duration of background tasks.       |
