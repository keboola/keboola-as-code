package apphandler

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
)

func newTestManager() *Manager {
	return &Manager{
		handlers: syncmap.New[handlerKey, appHandlerWrapper](func(handlerKey) *appHandlerWrapper {
			return &appHandlerWrapper{lock: &sync.Mutex{}}
		}),
	}
}

func TestHandlerKeyFor(t *testing.T) {
	t.Parallel()

	ref := k8sapp.WorkloadRef{AppID: "123"}
	hub := appconfig.AppConfigResult{Workload: ref}
	custom := appconfig.AppConfigResult{Workload: ref, AppConfig: api.AppConfig{CustomHost: "data.customer.org"}}

	assert.Equal(t, handlerKey{Workload: ref}, handlerKeyFor(hub))
	assert.Equal(t, handlerKey{Workload: ref, Host: "data.customer.org"}, handlerKeyFor(custom))
	assert.NotEqual(t, handlerKeyFor(hub), handlerKeyFor(custom))
}

func TestManager_EvictWorkloadDropsEveryHost(t *testing.T) {
	t.Parallel()

	m := newTestManager()
	ref := k8sapp.WorkloadRef{AppID: "123"}
	other := k8sapp.WorkloadRef{AppID: "456"}

	cancelled := map[handlerKey]bool{}
	for _, key := range []handlerKey{{Workload: ref}, {Workload: ref, Host: "a.customer.org"}, {Workload: other}} {
		w := m.handlers.GetOrInit(key)
		w.handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
		w.cancel = func(error) { cancelled[key] = true }
	}

	m.evictWorkload(ref)

	assert.True(t, cancelled[handlerKey{Workload: ref}])
	assert.True(t, cancelled[handlerKey{Workload: ref, Host: "a.customer.org"}])
	assert.False(t, cancelled[handlerKey{Workload: other}])
	assert.NotNil(t, m.handlers.GetOrInit(handlerKey{Workload: other}).handler)
}

func TestManager_EvictHostKeepsOtherHosts(t *testing.T) {
	t.Parallel()

	m := newTestManager()
	ref := k8sapp.WorkloadRef{AppID: "123"}
	hubKey := handlerKey{Workload: ref}
	customKey := handlerKey{Workload: ref, Host: "a.customer.org"}

	cancelled := false
	for _, key := range []handlerKey{hubKey, customKey} {
		w := m.handlers.GetOrInit(key)
		w.handler = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
		w.cancel = func(error) { cancelled = cancelled || key == customKey }
	}

	m.EvictHost(ref, "a.customer.org")

	assert.True(t, cancelled)
	assert.Nil(t, m.handlers.GetOrInit(customKey).handler)
	assert.NotNil(t, m.handlers.GetOrInit(hubKey).handler)
}

func TestAppHandler_CanonicalRedirect(t *testing.T) {
	t.Parallel()

	publicURL := &url.URL{Scheme: "https", Host: "hub.example.com"}
	slug := "my-app"
	newHandler := func(customHost string) *appHandler {
		app := api.AppConfig{ID: "123", AppSlug: &slug, CustomHost: customHost}
		return &appHandler{app: app, workload: k8sapp.WorkloadRef{AppID: "123"}, baseURL: app.BaseURL(publicURL)}
	}
	newReq := func(host string) *http.Request {
		req := httptest.NewRequest(http.MethodGet, "http://"+host+"/path?q=1", nil)
		req.Host = host
		return req
	}

	t.Run("hub: other host is redirected", func(t *testing.T) {
		t.Parallel()
		loc, ok := newHandler("").canonicalRedirect(newReq("123.hub.example.com"))
		require.True(t, ok)
		assert.Equal(t, "https://my-app-123.hub.example.com/path?q=1", loc)
	})

	t.Run("hub: canonical host is served", func(t *testing.T) {
		t.Parallel()
		_, ok := newHandler("").canonicalRedirect(newReq("my-app-123.hub.example.com"))
		assert.False(t, ok)
	})

	t.Run("custom host is never redirected", func(t *testing.T) {
		t.Parallel()
		_, ok := newHandler("data.customer.org").canonicalRedirect(newReq("data.customer.org"))
		assert.False(t, ok)
		_, ok = newHandler("data.customer.org").canonicalRedirect(newReq("data.customer.org:443"))
		assert.False(t, ok)
	})

	t.Run("spoofed X-Forwarded-Host does not change the target", func(t *testing.T) {
		t.Parallel()
		req := newReq("123.hub.example.com")
		req.Header.Set("X-Forwarded-Host", "evil.example.net")
		loc, ok := newHandler("").canonicalRedirect(req)
		require.True(t, ok)
		assert.Equal(t, "https://my-app-123.hub.example.com/path?q=1", loc)
	})
}
