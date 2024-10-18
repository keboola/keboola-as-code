package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dimfeld/httptreemux/v5"
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
	grp.GET("/ignored-1", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	grp.GET("/ignored-2", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	grp.GET("/action", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Register middleware
	cfg := middleware.NewConfig(
		middleware.WithFilter(func(req *http.Request) bool { return req.URL.Path != "/api/ignored-1" }),
		middleware.WithFilterAccessLog(func(req *http.Request) bool { return req.URL.Path != "/api/ignored-2" }),
	)
	logger := log.NewDebugLogger()
	handler = middleware.Wrap(handler, middleware.RequestInfo(), middleware.Filter(cfg), middleware.Logger(logger))

	// Send logged request
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/action", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	// Send ignored requests
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/ignored-1", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/api/ignored-2", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	// Assert
	expected := `{"level":"info","message":"req /api/action status=200 bytes=2 time=%s client_ip=192.0.2.1 agent=my-user-agent","component":"http","http.request_id":"%s"}`
	logger.AssertJSONMessages(t, expected)
}
