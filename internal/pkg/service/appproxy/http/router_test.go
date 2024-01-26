package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/dependencies"
)

var apps = []DataApp{
	{
		ID:           "123",
		Name:         "Test app",
		UpstreamHost: "123.keboola.local",
		Provider:     nil,
	},
}

func TestAppProxyRouter(t *testing.T) {
	t.Parallel()

	d, _ := proxyDependencies.NewMockedServiceScope(t, config.NewConfig())

	// Create dummy handler
	router := NewRouter(context.Background(), d, apps)
	handler := router.CreateHandler()

	// Request without app id
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "https://data-apps.keboola.local/", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, `Unable to parse application ID from the URL.`, rec.Body.String())

	// Request to unknown app
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "https://unknown.data-apps.keboola.local/", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Equal(t, `Application "unknown" not found.`, rec.Body.String())

	// Request to known app
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "https://123.data-apps.keboola.local/", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusBadGateway, rec.Code)
	assert.Equal(t, "", rec.Body.String())
}
