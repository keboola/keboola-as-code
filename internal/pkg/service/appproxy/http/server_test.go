package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/config"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appproxy/dependencies"
)

func TestAppProxyHandler(t *testing.T) {
	t.Parallel()

	d, mocked := proxyDependencies.NewMockedServiceScope(t, config.NewConfig())

	// Create dummy handler
	handler := newHandler(d.Logger(), d.Telemetry())

	// Send logged request
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/path", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	// Send ignored request
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/health-check", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	assert.NotNil(t, mocked.DebugLogger().CompareJSONMessages(`{"level":"info","message":"req /health-check %A"}`))
	mocked.DebugLogger().AssertJSONMessages(t, `{"level":"info","message":"req /path %A","http.request_id":"%s","component":"http"}`)

	metrics := mocked.TestTelemetry().Metrics(t)
	assert.Len(t, metrics, 3)
	assert.Equal(t, "keboola.go.http.server.request_content_length", metrics[0].Name)
	assert.Equal(t, "keboola.go.http.server.response_content_length", metrics[1].Name)
	assert.Equal(t, "keboola.go.http.server.duration", metrics[2].Name)
}
