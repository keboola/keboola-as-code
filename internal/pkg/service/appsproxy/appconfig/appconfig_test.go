package appconfig_test

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/appconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type testCase struct {
	name     string
	appID    string
	attempts []attempt
}

type attempt struct {
	delay             time.Duration
	responses         []*http.Response
	expectedErrorCode int
	expectedConfig    appconfig.AppProxyConfig
	expectedModified  bool
}

func TestLoader_LoadConfig(t *testing.T) {
	testCases := []testCase{
		{
			name:  "unknown",
			appID: "1",
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
			name:  "server-error",
			appID: "2",
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
			name:  "retry",
			appID: "3",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "3",
						UpstreamAppURL: "http://app.local",
					},
					expectedModified: true,
				},
			},
		},
		{
			name:  "cache-valid",
			appID: "4",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "4",
						UpstreamAppURL: "http://app.local",
					},
					expectedModified: true,
				},
				{
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "4",
						UpstreamAppURL: "http://app.local",
					},
				},
			},
		},
		{
			name:  "etag-match",
			appID: "5",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "5",
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
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "5",
						UpstreamAppURL: "http://app.local",
					},
				},
				{
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "5",
						UpstreamAppURL: "http://app.local",
					},
				},
				{
					delay: 31 * time.Second,
					responses: []*http.Response{
						newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
					},
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "5",
						UpstreamAppURL: "http://app.local",
					},
				},
			},
		},
		{
			name:  "etag-mismatch",
			appID: "6",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "6",
						UpstreamAppURL: "http://app.local",
					},
					expectedModified: true,
				},
				{
					delay: 10 * time.Minute,
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://new-app.local"}, `"etag-new-value"`, "max-age=60"),
					},
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "6",
						UpstreamAppURL: "http://new-app.local",
					},
					expectedModified: true,
				},
				{
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "6",
						UpstreamAppURL: "http://new-app.local",
					},
				},
			},
		},
		{
			name:  "etag-error",
			appID: "7",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "7",
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
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "7",
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
			name:  "max-expiration",
			appID: "8",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=7200"),
					},
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "8",
						UpstreamAppURL: "http://app.local",
					},
					expectedModified: true,
				},
				{
					delay: 59 * time.Minute,
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "8",
						UpstreamAppURL: "http://app.local",
					},
				},
				{
					delay: 2 * time.Minute,
					responses: []*http.Response{
						newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
					},
					expectedConfig: appconfig.AppProxyConfig{
						ID:             "8",
						UpstreamAppURL: "http://app.local",
					},
				},
			},
		},
	}

	t.Parallel()

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			clk := clock.NewMock()
			transport := httpmock.NewMockTransport()

			url := "https://sandboxes.keboola.com"

			httpClient := client.NewTestClient().WithRetry(client.TestingRetry()).WithTransport(transport)

			loader := appconfig.NewSandboxesAPILoader(log.NewDebugLogger(), clk, httpClient, url, "")

			for i, attempt := range tc.attempts {
				t.Logf("attempt %d/%d", i+1, len(tc.attempts))
				transport.Reset()

				clk.Add(attempt.delay)

				transport.RegisterResponder(
					http.MethodGet,
					fmt.Sprintf("%s/apps/%s/proxy-config", url, tc.appID),
					httpmock.ResponderFromMultipleResponses(attempt.responses, t.Log),
				)

				config, modified, err := loader.LoadConfig(context.Background(), tc.appID)
				if attempt.expectedErrorCode != 0 {
					require.Error(t, err)
					var sandboxesError *appconfig.SandboxesError
					errors.As(err, &sandboxesError)
					assert.Equal(t, attempt.expectedErrorCode, sandboxesError.StatusCode())
				} else {
					require.NoError(t, err)
					assert.Equal(t, attempt.expectedConfig.ID, config.ID)
					assert.Equal(t, attempt.expectedConfig.Name, config.Name)
					assert.Equal(t, attempt.expectedConfig.UpstreamAppURL, config.UpstreamAppURL)
					assert.Equal(t, attempt.expectedConfig.AuthProviders, config.AuthProviders)
					assert.Equal(t, attempt.expectedConfig.AuthRules, config.AuthRules)
				}
				assert.Equal(t, attempt.expectedModified, modified)

				assert.Equal(t, len(attempt.responses), transport.GetTotalCallCount())
			}
		})
	}
}

func TestLoader_LoadConfig_Race(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()
	transport := httpmock.NewMockTransport()

	url := "https://sandboxes.keboola.com"

	responses := []*http.Response{
		newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
		newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
		newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
		newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
		newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
		newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
		newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
		newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
		newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://app.local"}, `"etag-value"`, "max-age=60"),
		newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
	}

	transport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/apps/%s/proxy-config", url, "test"),
		httpmock.ResponderFromMultipleResponses(responses, t.Log),
	)

	httpClient := client.NewTestClient().WithRetry(client.TestingRetry()).WithTransport(transport)

	loader := appconfig.NewSandboxesAPILoader(log.NewDebugLogger(), clk, httpClient, url, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}
	counter := atomic.NewInt64(0)
	// Load configuration 10x in parallel
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			config, _, err := loader.LoadConfig(ctx, "test")
			assert.NoError(t, err)

			assert.Equal(t, "http://app.local", config.UpstreamAppURL)

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

func TestLoader_Notify(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()
	transport := httpmock.NewMockTransport()

	url := "https://sandboxes.keboola.com"
	appID := "app"

	transport.RegisterResponder(
		http.MethodPatch,
		fmt.Sprintf("%s/apps/%s", url, appID),
		httpmock.NewStringResponder(http.StatusOK, ""),
	)

	loader := appconfig.NewSandboxesAPILoader(log.NewDebugLogger(), clk, client.New().WithTransport(transport), url, "")
	err := loader.Notify(context.Background(), appID)
	assert.NoError(t, err)
	err = loader.Notify(context.Background(), appID)
	assert.NoError(t, err)

	assert.Equal(t, 1, transport.GetTotalCallCount())
}

func TestLoader_Notify_Race(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()
	transport := httpmock.NewMockTransport()

	url := "https://sandboxes.keboola.com"

	transport.RegisterResponder(
		http.MethodPatch,
		fmt.Sprintf("%s/apps/%s", url, "test"),
		httpmock.NewStringResponder(http.StatusOK, "{}").Times(10, t.Log),
	)

	httpClient := client.NewTestClient().WithRetry(client.TestingRetry()).WithTransport(transport)

	loader := appconfig.NewSandboxesAPILoader(log.NewDebugLogger(), clk, httpClient, url, "")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wg := sync.WaitGroup{}
	counter := atomic.NewInt64(0)
	// Load configuration 10x in parallel
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := loader.Notify(ctx, "test")
			assert.NoError(t, err)

			counter.Add(1)
		}()
	}

	// Wait for all requests
	wg.Wait()

	// Check total requests count
	assert.Equal(t, int64(10), counter.Load())

	assert.Equal(t, 1, transport.GetTotalCallCount())
}
