package http

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/dependencies"
)

func TestAppProxyHandler(t *testing.T) {
	t.Parallel()

	d, mocked := proxyDependencies.NewMockedServiceScope(t, config.NewConfig())

	// Create dummy handler
	handler := newHandler(d.Logger(), d.Telemetry())

	// Send logged request
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/path", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	// Send ignored request
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/health-check", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	assert.NotNil(t, mocked.DebugLogger().CompareJSONMessages(`{"level":"info","message":"req /health-check %A"}`))
	mocked.DebugLogger().AssertJSONMessages(t, `{"level":"info","message":"req /path %A","http.request_id":"%s","component":"http"}`)

	attributes := attribute.NewSet(
		attribute.String("http.method", "GET"),
		attribute.String("http.scheme", "http"),
		attribute.Int("http.status_code", 200),
		attribute.String("net.host.name", "example.com"),
	)

	mocked.TestTelemetry().AssertMetrics(
		t,
		[]metricdata.Metrics{
			{
				Name:        "keboola.go.http.server.request_content_length",
				Description: "",
				Unit:        "",
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
				Name:        "keboola.go.http.server.response_content_length",
				Description: "",
				Unit:        "",
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
				Description: "",
				Unit:        "",
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
