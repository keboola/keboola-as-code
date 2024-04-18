package appconfig_test

import (
	"context"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type testCase struct {
	name     string
	attempts []attempt
}

type attempt struct {
	delay             time.Duration
	responses         []*http.Response
	expectedErrorCode int
	expectedConfig    api.AppConfig
	expectedModified  bool
}

func TestLoader_LoadConfig(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("windows doesn't have /etc/resolv.conf")
	}

	testCases := []testCase{
		{
			name: "not-found",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 404, map[string]any{}, "", ""),
					},
					expectedErrorCode: 404,
				},
			},
		},
		{
			name: "server-error",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
					},
					expectedErrorCode: 500,
				},
			},
		},
		{
			name: "retry",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
					expectedModified: true,
				},
			},
		},
		{
			name: "cache-valid",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
					expectedModified: true,
				},
				{
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
				},
			},
		},
		{
			name: "etag-match",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
					expectedModified: true,
				},
				{
					delay: 10 * time.Minute,
					responses: []*http.Response{
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
					},
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
				},
				{
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
				},
				{
					delay: 31 * time.Second,
					responses: []*http.Response{
						newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
					},
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
				},
			},
		},
		{
			name: "etag-mismatch",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
					expectedModified: true,
				},
				{
					delay: 10 * time.Minute,
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://new-app.local"}, `"etag-new-value"`, "max-age=60"),
					},
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://new-app.local",
					},
					expectedModified: true,
				},
				{
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://new-app.local",
					},
				},
			},
		},
		{
			name: "etag-error",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
					expectedModified: true,
				},
				{
					delay: 10 * time.Minute,
					responses: []*http.Response{
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
					},
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
				},
				{
					delay: time.Hour,
					responses: []*http.Response{
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 500, map[string]any{}, "", ""),
					},
					expectedErrorCode: 500,
				},
			},
		},
		{
			name: "max-expiration",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=7200"),
					},
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
					expectedModified: true,
				},
				{
					delay: 59 * time.Minute,
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
				},
				{
					delay: 2 * time.Minute,
					responses: []*http.Response{
						newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
					},
					expectedConfig: api.AppConfig{
						ID:             "test",
						UpstreamAppURL: "http://app.local",
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			clk := clock.NewMock()
			d, mock := dependencies.NewMockedServiceScope(t, config.New(), commonDeps.WithClock(clk))

			appID := api.AppID("test")
			transport := mock.MockedHTTPTransport()
			loader := d.AppConfigLoader()

			for i, attempt := range tc.attempts {
				t.Logf("attempt %d/%d", i+1, len(tc.attempts))
				clk.Add(attempt.delay)

				transport.Reset()
				transport.RegisterResponder(
					http.MethodGet,
					fmt.Sprintf("%s/apps/%s/proxy-config", mock.TestConfig().SandboxesAPI.URL, appID),
					httpmock.ResponderFromMultipleResponses(attempt.responses, t.Log),
				)

				cfg, modified, err := loader.GetConfig(ctx, appID)
				if attempt.expectedErrorCode != 0 {
					require.Error(t, err)
					var apiErr *api.Error
					errors.As(err, &apiErr)
					assert.Equal(t, attempt.expectedErrorCode, apiErr.StatusCode())
				} else {
					require.NoError(t, err)
					assert.Equal(t, attempt.expectedConfig.ID, cfg.ID)
					assert.Equal(t, attempt.expectedConfig.Name, cfg.Name)
					assert.Equal(t, attempt.expectedConfig.UpstreamAppURL, cfg.UpstreamAppURL)
					assert.Equal(t, attempt.expectedConfig.AuthProviders, cfg.AuthProviders)
					assert.Equal(t, attempt.expectedConfig.AuthRules, cfg.AuthRules)
				}
				assert.Equal(t, attempt.expectedModified, modified)
				assert.Equal(t, len(attempt.responses), transport.GetTotalCallCount())
			}
		})
	}
}

func TestLoader_LoadConfig_Race(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("windows doesn't have /etc/resolv.conf")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clk := clock.NewMock()
	d, mock := dependencies.NewMockedServiceScope(t, config.New(), commonDeps.WithClock(clk))

	appID := api.AppID("test")

	transport := mock.MockedHTTPTransport()
	transport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/apps/%s/proxy-config", mock.TestConfig().SandboxesAPI.URL, appID),
		httpmock.ResponderFromResponse(newResponse(t, http.StatusOK, map[string]any{"upstreamAppUrl": "http://app.local"}, "", "")),
	)

	loader := d.AppConfigLoader()

	wg := sync.WaitGroup{}
	counter := atomic.NewInt64(0)
	// Load configuration 10x in parallel
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			cfg, _, err := loader.GetConfig(ctx, appID)
			assert.NoError(t, err)
			assert.Equal(t, "http://app.local", cfg.UpstreamAppURL)
			counter.Add(1)
		}()
	}

	// Wait for all requests
	wg.Wait()

	// Check total requests count
	assert.Equal(t, int64(10), counter.Load())
}

func newResponse(t *testing.T, code int, body map[string]any, eTag string, cacheControl string) *http.Response {
	t.Helper()

	response, err := httpmock.NewJsonResponse(code, body)
	require.NoError(t, err)
	response.Header.Set("ETag", eTag)
	response.Header.Set("Cache-Control", cacheControl)
	return response
}
