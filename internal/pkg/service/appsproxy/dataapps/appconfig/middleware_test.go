package appconfig_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dimfeld/httptreemux/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// testResolver claims one hostname for a Sandbox, as the watcher does, and
// supplies the appId from that Sandbox's own spec.
type testResolver struct {
	sandboxHost  string
	sandboxName  string
	sandboxAppID api.AppID
}

func (r *testResolver) ResolveHost(_ context.Context, host string) (k8sapp.WorkloadRef, bool) {
	if r.sandboxHost != "" && host == r.sandboxHost {
		return k8sapp.WorkloadRef{AppID: r.sandboxAppID, SandboxName: r.sandboxName}, true
	}
	return k8sapp.WorkloadRef{}, false
}

type testLoader struct{}

func (l *testLoader) GetConfig(ctx context.Context, appID api.AppID) (out api.AppConfig, err error) {
	switch appID {
	case "1":
		return api.AppConfig{
			ID:             "1",
			Name:           "App 1",
			AppSlug:        new("app-1"),
			ProjectID:      "1",
			UpstreamAppURL: "https://internal.app-1.example.com",
		}, nil
	case "changed":
		return api.AppConfig{
			ID:             "2",
			Name:           "App 2",
			AppSlug:        new("app-2"),
			ProjectID:      "2",
			UpstreamAppURL: "https://internal.app-2.example.com",
		}, nil
	default:
		return api.AppConfig{}, errors.New("error")
	}
}

func TestAppConfigMiddleware(t *testing.T) {
	t.Parallel()

	handler, logger := testSetup(t)

	// Send logged request
	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app-1.example.com/api/action", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	// Assert
	expected := `{"level":"info","message":"req 200 https://app-1.example.com/api/action","component":"http","http.status":200,"http.bytes":2,"http.time":"%s","http.client.ip":"192.0.2.1","http.client.agent":"my-user-agent","http.request_id":"%s","proxy.app.id":"1","proxy.app.name":"App 1","proxy.app.projectId":"1","proxy.app.upstream":"https://internal.app-1.example.com","context.appId":"1"}`
	logger.AssertJSONMessages(t, expected)
}

// The resolved workload must reach the handler through AppConfigResult, or a
// draft request silently routes to the App's upstream.
func TestMiddleware_ResolvedWorkloadInContext(t *testing.T) {
	t.Parallel()

	// The hostname carries no appId at all — the Sandbox's own spec supplies it.
	resolver := &testResolver{sandboxHost: "draft-9f3c.example.com", sandboxName: "draft-9f3c", sandboxAppID: "1"}

	var got appconfig.AppConfigResult
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		got = appconfig.AppConfigFromContext(req.Context())
		w.WriteHeader(http.StatusOK)
	})
	handler = middleware.Wrap(
		handler,
		middleware.RequestInfo(),
		appconfig.Middleware(&testLoader{}, resolver, "example.com"),
	)

	get := func(url string) {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, url, nil))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// The exact-hostname match runs first and its appId loads the app config.
	get("https://draft-9f3c.example.com/")
	assert.Equal(t, k8sapp.WorkloadRef{AppID: "1", SandboxName: "draft-9f3c"}, got.Workload)
	assert.Equal(t, api.AppID("1"), got.AppID)
	require.NoError(t, got.Err)
	assert.Equal(t, api.AppID("1"), got.AppConfig.ID)

	// Everything else falls through to the unchanged App normalisation.
	get("https://app-1.example.com/")
	assert.Equal(t, k8sapp.WorkloadRef{AppID: "1"}, got.Workload)
	assert.Equal(t, api.AppID("1"), got.AppID)
}

// A hostname the Sandbox index claims but that is outside the proxy's own
// public domain must not route: parseAppID enforced that for the App path.
func TestMiddleware_ExactHostnameOutsidePublicDomainIsNotRouted(t *testing.T) {
	t.Parallel()

	resolver := &testResolver{sandboxHost: "draft-9f3c.evil.test", sandboxName: "draft-9f3c", sandboxAppID: "1"}

	var got appconfig.AppConfigResult
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		got = appconfig.AppConfigFromContext(req.Context())
		w.WriteHeader(http.StatusOK)
	})
	handler = middleware.Wrap(
		handler,
		middleware.RequestInfo(),
		appconfig.Middleware(&testLoader{}, resolver, "example.com"),
	)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://draft-9f3c.evil.test/", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, got.Workload.SandboxName)
	assert.Empty(t, got.AppID)
}

func testSetup(t *testing.T) (http.Handler, log.DebugLogger) {
	t.Helper()

	// Create dummy handler
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.NotEmpty(t, appconfig.AppConfigFromContext(req.Context()).AppConfig.ID)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Create dummy handler
	mux := httptreemux.NewContextMux()
	grp := mux.NewGroup("/api")
	grp.GET("/action", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Register middleware
	logger := log.NewDebugLogger()
	handler = middleware.Wrap(
		handler,
		middleware.RequestInfo(),
		appconfig.Middleware(&testLoader{}, &testResolver{}, "example.com"),
		middleware.Logger(logger),
	)
	return handler, logger
}
