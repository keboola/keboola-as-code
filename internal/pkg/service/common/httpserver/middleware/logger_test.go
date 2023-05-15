package middleware_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

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

	// Register middleware
	logger := log.NewDebugLogger()
	handler = middleware.Wrap(handler, middleware.RequestInfo(), middleware.Logger(logger))

	// Send request
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)

	// Assert
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())
	wildcards.Assert(t, `
[http][requestId=%s]INFO  request GET / 192.0.2.1 my-user-agent
[http][requestId=%s]INFO  response status=200 bytes=2 time=%s agent=my-user-agent
`, logger.AllMessages())
}
