package appconfig_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dimfeld/httptreemux/v5"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/httpserver/middleware"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type testLoader struct{}

func (l *testLoader) GetConfig(ctx context.Context, appID api.AppID) (out api.AppConfig, modified bool, err error) {
	switch appID {
	case "1":
		return api.AppConfig{
			ID:             "1",
			Name:           "App 1",
			AppSlug:        ptr.Ptr("app-1"),
			ProjectID:      "1",
			UpstreamAppURL: "https://internal.app-1.example.com",
		}, false, nil
	case "changed":
		return api.AppConfig{
			ID:             "2",
			Name:           "App 2",
			AppSlug:        ptr.Ptr("app-2"),
			ProjectID:      "2",
			UpstreamAppURL: "https://internal.app-2.example.com",
		}, true, nil
	default:
		return api.AppConfig{}, false, errors.New("error")
	}
}

func TestAppConfigMiddleware(t *testing.T) {
	t.Parallel()

	// Create dummy handler
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		assert.NotEqual(t, "", appconfig.AppConfigFromContext(req.Context()).AppConfig.ID)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Create dummy handler
	mux := httptreemux.NewContextMux()
	grp := mux.NewGroup("/api")
	grp.GET("/action", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	// Register middleware
	logger := log.NewDebugLogger()
	handler = middleware.Wrap(
		handler,
		middleware.RequestInfo(),
		appconfig.Middleware(&testLoader{}, "example.com"),
		middleware.Logger(logger),
	)

	// Send logged request
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "https://app-1.example.com/api/action", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK", rec.Body.String())

	// Assert
	expected := `{"level":"info","message":"req https://app-1.example.com/api/action status=200 bytes=2 time=%s client_ip=192.0.2.1 agent=my-user-agent","component":"http","http.request_id":"%s","proxy.app.id":"1","proxy.app.name":"App 1","proxy.app.projectId":"1","proxy.app.upstream":"https://internal.app-1.example.com"}`
	logger.AssertJSONMessages(t, expected)
}
