package prometheus_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry/metric/prometheus"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
)

func TestServeMetrics(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	d := dependencies.NewMocked(t, ctx)

	// Serve metrics
	port := netutils.FreePortForTest(t)
	listenAddr := fmt.Sprintf("localhost:%d", port)
	endpointURL := fmt.Sprintf(`http://%s/%s`, listenAddr, prometheus.Endpoint)
	provider, err := prometheus.ServeMetrics(ctx, prometheus.Config{Listen: listenAddr}, d.Logger(), d.Process(), "my-service")
	require.NoError(t, err)

	// Get metrics, no meter
	wildcards.Assert(t, `
# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="my-service"} 1
`, getBody(t, ctx, endpointURL))

	// Setup a meter
	meter := provider.Meter("test_meter")
	counter, err := meter.Float64Counter("foo", metric.WithDescription("a simple counter"))
	require.NoError(t, err)

	// Get metrics, empty meter
	wildcards.Assert(t, `
# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="my-service"} 1
`, getBody(t, ctx, endpointURL))

	// Add some value
	counter.Add(t.Context(), 5, metric.WithAttributes(
		attribute.Key("A").String("B"),
		attribute.Key("C").String("D"),
	))

	// Get metrics, meter with a value
	wildcards.Assert(t, `
# HELP foo_total a simple counter
# TYPE foo_total counter
foo_total{A="B",C="D"} 5
# HELP target_info Target metadata
# TYPE target_info gauge
target_info{service_name="my-service"} 1
`, getBody(t, ctx, endpointURL))

	// Shutdown the server
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// Check logs
	expected := `
{"level":"info","message":"starting HTTP server on \"%s\""}
{"level":"info","message":"started HTTP server on \"%s\", endpoint \"metrics\"\""}
{"level":"info","message":"exiting (bye bye)"}
{"level":"info","message":"shutting down HTTP server at \"localhost:%d\"","component":"metrics"}
{"level":"info","message":"HTTP server shutdown finished","component":"metrics"}
{"level":"info","message":"exited"}
`
	d.DebugLogger().AssertJSONMessages(t, expected)
}

func getBody(t *testing.T, ctx context.Context, url string) string {
	t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	require.NoError(t, err)
	res, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	return string(body)
}
