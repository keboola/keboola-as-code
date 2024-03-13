package http

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
)

func TestAppProxyHandler(t *testing.T) {
	t.Parallel()

	d, mocked := proxyDependencies.NewMockedServiceScope(t, config.New())

	// Create dummy handler
	handler := newHandler(
		d.Logger(),
		d.Telemetry(),
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprint(w, "OK")
		}),
		&url.URL{
			Host: "hub.keboola.local",
		},
	)

	// Send logged request
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "https://123.hub.keboola.local/path", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	// Send ignored request
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "https://123.hub.keboola.local/health-check", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	assert.NotNil(t, mocked.DebugLogger().CompareJSONMessages(`{"level":"info","message":"req https://123.hub.keboola.local/health-check %A"}`))
	mocked.DebugLogger().AssertJSONMessages(t, `{"level":"info","message":"req https://123.hub.keboola.local/path %A","http.request_id":"%s","component":"http"}`)

	attributes := attribute.NewSet(
		attribute.String("http.method", "GET"),
		attribute.String("http.scheme", "https"),
		attribute.Int("http.status_code", 200),
		attribute.String("net.host.name", "123.hub.keboola.local"),
		attribute.String("proxy.appid", "123"),
	)

	mocked.TestTelemetry().AssertMetrics(
		t,
		[]metricdata.Metrics{
			{
				Name:        "keboola.go.http.server.request.size",
				Description: "Measures the size of HTTP request messages.",
				Unit:        "By",
				Data: metricdata.Sum[int64]{
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Attributes: attributes,
							StartTime:  time.Time{},
							Time:       time.Time{},
							Value:      0,
							Exemplars:  nil,
						},
					},
					Temporality: 1,
					IsMonotonic: true,
				},
			},
			{
				Name:        "keboola.go.http.server.response.size",
				Description: "Measures the size of HTTP response messages.",
				Unit:        "By",
				Data: metricdata.Sum[int64]{
					DataPoints: []metricdata.DataPoint[int64]{
						{
							Attributes: attributes,
							StartTime:  time.Time{},
							Time:       time.Time{},
							Value:      2,
							Exemplars:  nil,
						},
					},
					Temporality: 1,
					IsMonotonic: true,
				},
			},
			{
				Name:        "keboola.go.http.server.duration",
				Description: "Measures the duration of inbound HTTP requests.",
				Unit:        "ms",
				Data: metricdata.Histogram[float64]{
					DataPoints: []metricdata.HistogramDataPoint[float64]{
						{
							Attributes:   attributes,
							StartTime:    time.Time{},
							Time:         time.Time{},
							Count:        1,
							Bounds:       []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000},
							BucketCounts: nil,
							Min:          metricdata.Extrema[float64]{},
							Max:          metricdata.Extrema[float64]{},
							Sum:          0,
							Exemplars:    nil,
						},
					},
					Temporality: 1,
				},
			},
		},
	)
}
