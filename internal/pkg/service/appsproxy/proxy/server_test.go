package proxy_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/testutil"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/server"
)

type portManager struct{}

func newZeroPortManager() server.PortManager {
	return &portManager{}
}

func (p portManager) GeneratePorts(ctx context.Context) {}

func (p portManager) GetFreePort() int {
	return 0
}

func TestAppProxyHandler(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	// Start app
	pm := newZeroPortManager()
	appServer := testutil.StartAppServer(t, pm)
	defer appServer.Close()

	// Start api
	appsAPI := testutil.StartDataAppsAPI(t, pm)
	defer appsAPI.Close()

	// Configure proxy
	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.SandboxesAPI.URL = appsAPI.URL
	cfg.CsrfTokenSalt = "abc"

	// Create dependencies
	d, mocked := proxyDependencies.NewMockedServiceScope(t, ctx, cfg, dependencies.WithRealHTTPClient())

	// Register apps
	appURL := testutil.AddAppDNSRecord(t, appServer, mocked.TestDNSServer())
	appsAPI.Register([]api.AppConfig{
		{
			ID:             "123",
			Name:           "public",
			AppSlug:        ptr.Ptr("PUBLIC"),
			ProjectID:      "456",
			UpstreamAppURL: appURL.String(),
			AuthRules: []api.Rule{
				{
					Type:         api.RulePathPrefix,
					Value:        "/",
					AuthRequired: ptr.Ptr(false),
				},
			},
		},
	})

	// Create proxy handler
	handler := proxy.NewHandler(ctx, d)

	// Get robots.txt
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "https://hub.keboola.local/robots.txt", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Disallow: /")

	// Get missing asset
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "https://hub.keboola.local/_proxy/assets/foo.bar", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)

	// Invalid host
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "https://public-123.foo.bar.local/path", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "Unexpected domain, missing application ID.")

	// Send logged request
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "https://public-123.hub.keboola.local/path", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "Hello, client", rec.Body.String())

	// Send ignored request
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "https://hub.keboola.local/health-check", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK\n", rec.Body.String())

	mocked.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"req 404 https://hub.keboola.local/_proxy/assets/foo.bar","http.request_id":"%s","component":"http"}
{"level":"warn","message":"badRequest: unexpected domain, missing application ID %A","http.request_id":"%s"}
{"level":"info","message":"req 400 https://public-123.foo.bar.local/path","http.request_id":"%s","component":"http"}
{"level":"info","message":"req 200 https://public-123.hub.keboola.local/path","http.request_id":"%s","component":"http"}
`)

	// HTTP server metrics are disabled via noop MeterProvider (they duplicate Datadog APM functionality)
	actualMetricsJSON := mocked.TestTelemetry().MetricsJSONString(
		t,
		telemetry.WithMetricFilter(func(metric metricdata.Metrics) bool {
			return strings.HasPrefix(metric.Name, "keboola.")
		}),
		telemetry.WithDataPointSortKey(func(attrs attribute.Set) string {
			host, _ := attrs.Value("server.address")
			status, _ := attrs.Value("http.response.status_code")
			return fmt.Sprintf("%d:%s", status.AsInt64(), host.AsString())
		}),
	)
	assert.Equal(t, "null", strings.TrimSpace(actualMetricsJSON))
}
