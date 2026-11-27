package basicauth

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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

type deps struct{}

func (deps) Clock() clockwork.Clock { return clockwork.NewRealClock() }
func (deps) Logger() log.Logger     { return log.NewNopLogger() }

func TestHandler_LoginOnCustomHost(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, customHost, host string
		hostOnly               bool
	}{
		{"hub", "", "app-123.hub.keboola.local", false},
		{"custom", "data.customer.org", "data.customer.org", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := config.New()
			cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
			cfg.CsrfTokenSalt = "salt"
			pw, err := pagewriter.New(deps{})
			require.NoError(t, err)
			app := api.AppConfig{ID: "123", CustomHost: tc.customHost}
			upstreamCalled := false
			h := NewHandler(log.NewNopLogger(), cfg, clockwork.NewRealClock(), pw, app, provider.Basic{Password: "pw"}, upstreamFunc(func(http.ResponseWriter, *http.Request) error {
				upstreamCalled = true
				return nil
			}))

			body := strings.NewReader(url.Values{"password": {"pw"}}.Encode())
			req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "https://"+tc.host+formPagePath+"?rd=/dash", body)
			req.Host = tc.host
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			req.Header.Set("X-Forwarded-Host", "evil.example.net")
			rec := httptest.NewRecorder()
			require.NoError(t, h.ServeHTTPOrError(rec, req))

			require.Equal(t, http.StatusMovedPermanently, rec.Code)
			assert.Equal(t, "https://"+tc.host+"/dash", rec.Header().Get("Location"))
			assert.False(t, upstreamCalled)
			cookies := rec.Result().Cookies()
			require.Len(t, cookies, 1)
			assert.Equal(t, tc.hostOnly, cookies[0].Domain == "", "cookie domain %q", cookies[0].Domain)
		})
	}
}

type upstreamFunc func(http.ResponseWriter, *http.Request) error

func (f upstreamFunc) ServeHTTPOrError(w http.ResponseWriter, r *http.Request) error { return f(w, r) }
