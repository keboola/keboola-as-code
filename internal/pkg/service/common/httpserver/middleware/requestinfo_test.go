package middleware_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	goaMiddleware "goa.design/goa/v3/middleware"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
)

func TestMetaMiddleware(t *testing.T) {
	t.Parallel()

	// Create dummy handler
	var reqCtx context.Context
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		reqCtx = req.Context()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Register middleware
	handler = middleware.Wrap(handler, middleware.RequestInfo())

	// Send request
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	handler.ServeHTTP(rec, req)

	// Assert
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())
	assert.NotEmpty(t, reqCtx.Value(middleware.RequestIDCtxKey))
	assert.NotEmpty(t, reqCtx.Value(goaMiddleware.RequestIDKey))
	assert.Equal(t, req.URL, reqCtx.Value(middleware.RequestURLCtxKey))
	assert.True(t, ctxattr.Attributes(reqCtx).HasValue("http.request_id"))
}
