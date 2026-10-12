package oauthproxy_test

import (
	"html"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/oauthproxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/authproxy/selector"
)

func TestHandler_InitErrorPageHidesProviderResponse(t *testing.T) {
	t.Parallel()

	const providerBody = "provider-response-body-7f3a"
	issuer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(providerBody))
	}))
	t.Cleanup(issuer.Close)

	d, mocked := dependencies.NewMockedServiceScope(t, t.Context(), config.New())
	app := api.AppConfig{ID: "app1"}
	auth := provider.OIDC{
		Base:         provider.Base{Info: provider.Info{ID: "oidc", Type: provider.TypeOIDC}},
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		IssuerURL:    issuer.URL,
	}

	handler := oauthproxy.NewHandler(d.Logger(), d.Config(), selector.New(d), d.PageWriter(), app, auth, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://app1.hub.keboola.local/", nil)
	err := handler.ServeHTTPOrError(rec, req)
	require.Error(t, err)
	d.PageWriter().WriteError(rec, req, &app, err)

	body := rec.Body.String()
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, body, html.EscapeString(`Application "app1" has invalid configuration for authentication provider "oidc"`))
	assert.NotContains(t, body, providerBody)
	assert.NotContains(t, body, issuer.Listener.Addr().String())
	assert.Contains(t, mocked.DebugLogger().AllMessages(), providerBody)
}
