package selector

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/pagewriter"
)

type testDeps struct{ cfg config.Config }

func (d testDeps) Clock() clockwork.Clock { return clockwork.NewRealClock() }
func (d testDeps) Logger() log.Logger     { return log.NewNopLogger() }
func (d testDeps) Config() config.Config  { return d.cfg }
func (d testDeps) PageWriter() *pagewriter.Writer {
	pw, _ := pagewriter.New(d)
	return pw
}

func newForApp(t *testing.T, customHost string) *SelectorForAppRule {
	t.Helper()
	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	rule, err := New(testDeps{cfg: cfg}).For(api.AppConfig{ID: "123", CustomHost: customHost}, map[provider.ID]Handler{"basic": nil})
	require.NoError(t, err)
	return rule
}

func TestSelector_CookieOnCustomHost(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, customHost, host, domain string
	}{
		{"hub", "", "app-123.hub.keboola.local", "app-123.hub.keboola.local"},
		{"custom", "data.customer.org", "data.customer.org", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+tc.host+"/", nil)
			req.Host = tc.host
			assert.Equal(t, tc.domain, newForApp(t, tc.customHost).cookie(req, tc.customHost != "", "basic", 0).Domain)
		})
	}
}

// The continuation page redirects to the origin of the handler, not to a forwarded host.
func TestSelector_CallbackContinuationOnCustomHost(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "https://data.customer.org"+proxyCallbackPath+"?code=x", nil)
	req.Host = "data.customer.org"
	req.Header.Set("X-Forwarded-Host", "evil.example.net")
	rec := httptest.NewRecorder()

	require.NoError(t, newForApp(t, "data.customer.org").ServeHTTPOrError(rec, req))

	assert.Contains(t, rec.Body.String(), "https://data.customer.org/_proxy/callback")
	assert.NotContains(t, rec.Body.String(), "evil.example.net")
}
