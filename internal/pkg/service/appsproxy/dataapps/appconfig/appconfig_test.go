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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
)

type testCase struct {
	name     string
	attempts []attempt
}

type attempt struct {
	delay             time.Duration
	responses         []*http.Response
	expectedErrorCode int
	expectedModified  bool
}

func TestLoader_LoadConfig(t *testing.T) {
	t.Parallel()

	appID := api.AppID("test")
	appPayload := map[string]any{
		"appId":          appID.String(),
		"appName":        "my-test",
		"projectId":      "123",
		"upstreamAppUrl": "http://app.local",
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
						newResponse(t, 200, appPayload, `"etag-value"`, "max-age=60"),
					},
					expectedErrorCode: 0, // no error expected
					expectedModified:  true,
				},
			},
		},
		{
			name: "cache-valid",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, appPayload, `"etag-value"`, "max-age=60"),
					},
					expectedErrorCode: 0, // no error expected
					expectedModified:  true,
				},
				{
					expectedErrorCode: 0, // no error expected
					expectedModified:  false,
				},
			},
		},
		{
			name: "etag-match",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, appPayload, `"etag-value"`, "max-age=60"),
					},
					expectedErrorCode: 0, // no error expected
					expectedModified:  true,
				},
				{
					delay: 10 * time.Minute,
					responses: []*http.Response{
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
					},
					expectedErrorCode: 0, // no error expected
					expectedModified:  false,
				},
				{
					expectedErrorCode: 0, // no error expected
					expectedModified:  false,
				},
				{
					delay: 31 * time.Second,
					responses: []*http.Response{
						newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
					},
					expectedErrorCode: 0, // no error expected
					expectedModified:  false,
				},
			},
		},
		{
			name: "etag-mismatch",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, appPayload, `"etag-value"`, "max-age=60"),
					},
					expectedErrorCode: 0, // no error expected
					expectedModified:  true,
				},
				{
					delay: 10 * time.Minute,
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"upstreamAppUrl": "http://new-app.local"}, `"etag-new-value"`, "max-age=60"),
					},
					expectedErrorCode: 0, // no error expected
					expectedModified:  true,
				},
				{
					expectedErrorCode: 0, // no error expected
					expectedModified:  false,
				},
			},
		},
		{
			name: "etag-error",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, appPayload, `"etag-value"`, "max-age=60"),
					},
					expectedErrorCode: 0, // no error expected
					expectedModified:  true,
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
					expectedErrorCode: 0, // no error expected
					expectedModified:  false,
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
					expectedModified:  false,
				},
			},
		},
		{
			name: "max-expiration",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, appPayload, `"etag-value"`, "max-age=7200"),
					},
					expectedErrorCode: 0, // no error expected
					expectedModified:  true,
				},
				{
					delay:             59 * time.Minute,
					expectedErrorCode: 0, // no error expected
					expectedModified:  false,
				},
				{
					delay: 2 * time.Minute,
					responses: []*http.Response{
						newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
					},
					expectedErrorCode: 0, // no error expected
					expectedModified:  false,
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			clk := clock.NewMock()
			d, mock := dependencies.NewMockedServiceScope(t, ctx, config.New(), commonDeps.WithClock(clk))

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
					if assert.ErrorAs(t, err, &apiErr) {
						assert.Equal(t, attempt.expectedErrorCode, apiErr.StatusCode())
					}
				} else {
					require.NoError(t, err)
					assert.NotEmpty(t, cfg)
				}
				assert.Equal(t, attempt.expectedModified, modified)
				assert.Equal(t, len(attempt.responses), transport.GetTotalCallCount())
			}
		})
	}
}

func TestLoader_LoadConfig_Race(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clk := clock.NewMock()
	d, mock := dependencies.NewMockedServiceScope(t, ctx, config.New(), commonDeps.WithClock(clk))

	appID := api.AppID("test")
	appPayload := map[string]any{
		"appId":          appID.String(),
		"appName":        "my-test",
		"projectId":      "123",
		"upstreamAppUrl": "http://app.local",
	}

	transport := mock.MockedHTTPTransport()
	transport.RegisterResponder(
		http.MethodGet,
		fmt.Sprintf("%s/apps/%s/proxy-config", mock.TestConfig().SandboxesAPI.URL, appID),
		httpmock.ResponderFromResponse(newResponse(t, http.StatusOK, appPayload, "", "")),
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
