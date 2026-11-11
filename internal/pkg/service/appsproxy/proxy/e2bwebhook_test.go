package proxy_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

func TestE2bWebhookNotForwarded(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	var operatorCalls atomic.Int32
	operatorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		operatorCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer operatorServer.Close()

	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.CookieSecretSalt = "x"
	cfg.CsrfTokenSalt = "abc"
	cfg.SandboxesAPI.URL = "https://example"
	cfg.K8s = config.K8s{AppsNamespace: "keboola"}

	envs := env.Empty()
	envs.Set("APPS_PROXY_E2B_WEBHOOK_UPSTREAM_URL", operatorServer.URL)
	require.NoError(t, configmap.GenerateAndBind(configmap.GenerateAndBindConfig{
		EnvNaming: env.NewNamingConvention("APPS_PROXY_"),
		Envs:      envs,
	}, &cfg))

	d, _ := proxyDependencies.NewMockedServiceScope(t, ctx, cfg, dependencies.WithRealHTTPClient())
	handler := proxy.NewHandler(ctx, d)

	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/_proxy/api/v1/webhook/e2b", strings.NewReader(`{"sandboxId":"sb-1"}`))
	req.Host = "hub.keboola.local"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("e2b-signature", "some-signature")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Zero(t, operatorCalls.Load())
}
