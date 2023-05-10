package prometheus_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
)

func TestServeMetrics(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	d := dependencies.NewMockedDeps(t)

	port, err := netutils.FreePort()
	assert.NoError(t, err)

	// Serve metrics
	listenAddr := fmt.Sprintf("localhost:%d", port)
	endpointURL := fmt.Sprintf(`http://%s/%s`, listenAddr, prometheus.Endpoint)
	provider, err := prometheus.ServeMetrics(ctx, "my-service", listenAddr, d.Logger(), d.Process())
	assert.NoError(t, err)

	// Get metrics, no meter
	wildcards.Assert(t, `
# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="my-service",telemetry_sdk_language="go",telemetry_sdk_name="opentelemetry",telemetry_sdk_version="%s"} 1
`, getBody(t, ctx, endpointURL))

	// Setup a meter
	meter := provider.Meter("test_meter")
	counter, err := meter.Float64Counter("foo", metric.WithDescription("a simple counter"))
	assert.NoError(t, err)

	// Get metrics, empty meter
	wildcards.Assert(t, `
# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="my-service",telemetry_sdk_language="go",telemetry_sdk_name="opentelemetry",telemetry_sdk_version="%s"} 1
`, getBody(t, ctx, endpointURL))

	// Add some value
	counter.Add(context.Background(), 5, metric.WithAttributes(
		attribute.Key("A").String("B"),
		attribute.Key("C").String("D"),
	))

	// Get metrics, meter with a value
	wildcards.Assert(t, `
# HELP foo_total a simple counter
# TYPE foo_total counter
foo_total{A="B",C="D",otel_scope_name="test_meter",otel_scope_version=""} 5
# HELP otel_scope_info Instrumentation Scope metadata
# TYPE otel_scope_info gauge
otel_scope_info{otel_scope_name="test_meter",otel_scope_version=""} 1
# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="my-service",telemetry_sdk_language="go",telemetry_sdk_name="opentelemetry",telemetry_sdk_version="%s"} 1
`, getBody(t, ctx, endpointURL))

	// Shutdown the server
	d.Process().Shutdown(errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// Check logs
	wildcards.Assert(t, `
INFO  process unique id "%s"
[metrics]INFO  HTTP server listening on "localhost:%d/metrics"
INFO  exiting (bye bye)
[metrics]INFO  shutting down HTTP server at "localhost:%d"
[metrics]INFO  HTTP server shutdown finished
INFO  exited
`, d.DebugLogger().AllMessages())
}

func getBody(t *testing.T, ctx context.Context, url string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	assert.NoError(t, err)
	res, err := http.DefaultClient.Do(req)
	assert.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	assert.NoError(t, err)
	return string(body)
}
