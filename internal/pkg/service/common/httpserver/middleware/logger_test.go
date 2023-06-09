package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dimfeld/httptreemux/v5"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
)

func TestLoggerMiddleware(t *testing.T) {
	t.Parallel()

	// Create dummy handler
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Create dummy handler
	mux := httptreemux.NewContextMux()
	grp := mux.NewGroup("/api")
	grp.GET("/ignored", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	grp.GET("/action", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Register middleware
	filter := func(req *http.Request) bool { return req.URL.Path != "/api/ignored" }
	cfg := middleware.NewConfig(middleware.WithFilter(filter))
	logger := log.NewDebugLogger()
	handler = middleware.Wrap(handler, middleware.RequestInfo(), middleware.Filter(cfg), middleware.Logger(logger))

	// Send logged request
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/action", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	// Send ignored request
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/ignored", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	// Assert
	wildcards.Assert(t, `
[http][requestId=%s]INFO  request GET /api/action 192.0.2.1 my-user-agent
[http][requestId=%s]INFO  response status=200 bytes=2 time=%s agent=my-user-agent
`, logger.AllMessages())
}
