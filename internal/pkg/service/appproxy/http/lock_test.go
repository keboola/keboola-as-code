package http

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/appconfig"
	mockoidcCustom "github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/mockoidc"
	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"hash/fnv"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"sync"
	"testing"
	"time"
)

func TestLocks(t *testing.T) {
	t.Parallel()

	appServer := startAppServer(t)
	defer appServer.Close()

	oidcProvider := startOIDCProviderServer(t)
	defer oidcProvider.Shutdown()

	tsURL, err := url.Parse(appServer.URL)
	require.NoError(t, err)

	m := []*mockoidc.MockOIDC{oidcProvider}

	apps := []appconfig.AppProxyConfig{
		{
			ID:             "oidc",
			Name:           "OIDC Protected App",
			UpstreamAppURL: tsURL.String(),
			AuthProviders: []appconfig.AuthProvider{
				{
					ID:           "oidc",
					ClientID:     m[0].Config().ClientID,
					ClientSecret: m[0].Config().ClientSecret,
					Type:         appconfig.OIDCProvider,
					AllowedRoles: []string{"admin"},
					IssuerURL:    m[0].Issuer(),
				},
			},
			AuthRules: []appconfig.AuthRule{
				{
					Type:  appconfig.PathPrefix,
					Value: "/",
					Auth:  []string{"oidc"},
				},
			},
		},
	}

	service := startSandboxesService2(t, apps)
	defer service.Close()

	handler := createProxyHandler(t, service.URL)
	proxy := httptest.NewUnstartedServer(handler)
	proxy.EnableHTTP2 = true
	proxy.StartTLS()
	defer proxy.Close()

	proxyURL, err := url.Parse(proxy.URL)
	require.NoError(t, err)

	// -------------------------------------
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}
	counter := atomic.NewInt64(0)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			client := createHTTPClient(proxyURL)

			m[0].QueueUser(&mockoidcCustom.MockUser{
				Email:         "admin@keboola.com",
				EmailVerified: pointer(true),
				Groups:        []string{"admin"},
			})

			request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://oidc.data-apps.keboola.local/foo/bar", nil)
			assert.NoError(t, err)

			response, err := client.Do(request)
			assert.NoError(t, err)
			if assert.Equal(t, response.StatusCode, http.StatusOK) {
				counter.Add(1)
			}

			//dump, err := httputil.DumpResponse(response, true)
			//assert.NoError(t, err)
			//fmt.Println(string(dump))
		}()
	}

	// Wait for all requests
	wg.Wait()

	// Check total requests count
	assert.Equal(t, int64(100), counter.Load())

}

func startSandboxesService2(t *testing.T, apps []appconfig.AppProxyConfig) *sandboxesService {
	t.Helper()

	service := &sandboxesService{
		apps: make(map[string]appconfig.AppProxyConfig),
	}

	for _, app := range apps {
		service.apps[app.ID] = app
	}

	r := regexp.MustCompile("apps/([a-zA-Z0-9]+)/proxy-config")

	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		match := r.FindStringSubmatch(req.RequestURI)
		if len(match) < 2 {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, "{}")
			return
		}

		appID := match[1]
		app, ok := service.apps[appID]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			io.WriteString(w, "{}")
			return
		}

		// Calculate ETag (in this test we simply hash the name)
		h := fnv.New64a()
		_, err := h.Write([]byte(app.Name))
		assert.NoError(t, err)

		w.Header().Set("ETag", fmt.Sprintf(`"%x"`, h.Sum64()))
		w.WriteHeader(http.StatusOK)

		jsonData, err := json.Encode(app, true)
		assert.NoError(t, err)

		w.Write(jsonData)
	})

	ts := httptest.NewUnstartedServer(handler)
	ts.EnableHTTP2 = true
	ts.Start()

	service.Server = ts

	return service
}
