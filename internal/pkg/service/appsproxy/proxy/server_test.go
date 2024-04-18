package proxy_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/testutil"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestAppProxyHandler(t *testing.T) {
	t.Parallel()

	// Start app
	appServer := testutil.StartAppServer(t)
	defer appServer.Close()

	// Start DNS server
	dnsServer := testutil.StartDNSServer(t)
	defer func() {
		assert.NoError(t, dnsServer.Shutdown())
	}()

	// Register app DNS record
	appURL := testutil.AddAppDNSRecord(t, appServer, dnsServer)

	// Start api
	appsAPI := testutil.StartDataAppsAPI(t, []api.AppConfig{
		{
			ID:             "123",
			Name:           "public",
			UpstreamAppURL: appURL.String(),
			AuthRules: []api.Rule{
				{
					Type:         api.RulePathPrefix,
					Value:        "/",
					AuthRequired: pointer(false),
				},
			},
		},
	})
	defer appsAPI.Close()

	// Configure proxy
	cfg := config.New()
	cfg.DNSServer = dnsServer.Addr()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.SandboxesAPI.URL = appsAPI.URL

	d, mocked := proxyDependencies.NewMockedServiceScope(t, cfg, dependencies.WithRealHTTPClient())

	handler := proxy.NewHandler(d)

	// Get robots.txt
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "https://hub.keboola.local/robots.txt", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Disallow: /")

	// Get style.css
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "https://hub.keboola.local/_proxy/assets/style.css", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Body.String())

	// Get missing asset
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "https://hub.keboola.local/_proxy/assets/foo.bar", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)

	// Invalid host
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "https://123.foo.bar.local/path", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "Unexpected domain, missing application ID.")

	// Send logged request
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "https://123.hub.keboola.local/path", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "Hello, client", rec.Body.String())

	// Send ignored request
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "https://hub.keboola.local/health-check", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK\n", rec.Body.String())

	assert.NotNil(t, mocked.DebugLogger().CompareJSONMessages(`{"level":"info","message":"req https://123.hub.keboola.local/health-check %A"}`))
	mocked.DebugLogger().AssertJSONMessages(t, `{"level":"info","message":"req https://123.hub.keboola.local/path %A","http.request_id":"%s","component":"http"}`)

	// expectedHistogramBounds := []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000}
	// mocked.TestTelemetry().AssertMetrics(
	//	t,
	//	[]metricdata.Metrics{
	//		// Server metrics
	//		{
	//			Name:        "keboola.go.http.server.request.size",
	//			Description: "Measures the size of HTTP request messages.",
	//			Unit:        "By",
	//			Data: metricdata.Sum[int64]{
	//				DataPoints: []metricdata.DataPoint[int64]{
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("hub.keboola.local", 404, "", "", ""),
	//						Value:      0,
	//					},
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("hub.keboola.local", 200, "", "", ""),
	//						Value:      0,
	//					},
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("123.foo.bar.local", 400, "", "", ""),
	//						Value:      0,
	//					},
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("123.hub.keboola.local", 200, "123", "public", appURL.String()),
	//						Value:      0,
	//					},
	//				},
	//				Temporality: 1,
	//				IsMonotonic: true,
	//			},
	//		},
	//		{
	//			Name:        "keboola.go.http.server.response.size",
	//			Description: "Measures the size of HTTP response messages.",
	//			Unit:        "By",
	//			Data: metricdata.Sum[int64]{
	//				DataPoints: []metricdata.DataPoint[int64]{
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("hub.keboola.local", 404, "", "", ""),
	//						Value:      0,
	//					},
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("hub.keboola.local", 200, "", "", ""),
	//						Value:      0,
	//					},
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("123.foo.bar.local", 400, "", "", ""),
	//						Value:      0,
	//					},
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("123.hub.keboola.local", 200, "123", "public", appURL.String()),
	//						Value:      0,
	//					},
	//				},
	//				Temporality: 1,
	//				IsMonotonic: true,
	//			},
	//		},
	//		{
	//			Name:        "keboola.go.http.server.duration",
	//			Description: "Measures the duration of inbound HTTP requests.",
	//			Unit:        "ms",
	//			Data: metricdata.Histogram[float64]{
	//				DataPoints: []metricdata.HistogramDataPoint[float64]{
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("hub.keboola.local", 404, "", "", ""),
	//						Count:      1,
	//						Bounds:     expectedHistogramBounds,
	//					},
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("hub.keboola.local", 200, "", "", ""),
	//						Count:      1,
	//						Bounds:     expectedHistogramBounds,
	//					},
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("123.foo.bar.local", 400, "", "", ""),
	//						Count:      1,
	//						Bounds:     expectedHistogramBounds,
	//					},
	//					{
	//						Attributes: testutil.ExpectedServerReqAttrs("123.hub.keboola.local", 200, "123", "public", appURL.String()),
	//						Count:      1,
	//						Bounds:     expectedHistogramBounds,
	//					},
	//				},
	//				Temporality: 1,
	//			},
	//		},
	//	},
	//	telemetry.WithMetricFilter(func(metric metricdata.Metrics) bool {
	//		return strings.HasPrefix(metric.Name, "keboola.")
	//	}),
	//)
}
