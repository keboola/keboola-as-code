package appconfig

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/jarcoal/httpmock"
	"github.com/keboola/go-client/pkg/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
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
	expectedETag      string
	expectedErrorCode int
	expectedConfig    AppProxyConfig
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
						newResponse(t, 200, map[string]any{"id": "3", "upstreamAppHost": "app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: AppProxyConfig{
						ID:              "3",
						Name:            "3",
						UpstreamAppHost: "app.local",
						eTag:            `"etag-value"`,
						maxAge:          60 * time.Second,
					},
				},
			},
		},
		{
			name:  "cache-valid",
			appID: "4",
			attempts: []attempt{
				{
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"id": "4", "upstreamAppHost": "app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: AppProxyConfig{
						ID:              "4",
						Name:            "4",
						UpstreamAppHost: "app.local",
						eTag:            `"etag-value"`,
						maxAge:          60 * time.Second,
					},
				},
				{
					expectedConfig: AppProxyConfig{
						ID:              "4",
						Name:            "4",
						UpstreamAppHost: "app.local",
						eTag:            `"etag-value"`,
						maxAge:          60 * time.Second,
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
						newResponse(t, 200, map[string]any{"id": "5", "upstreamAppHost": "app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: AppProxyConfig{
						ID:              "5",
						Name:            "5",
						UpstreamAppHost: "app.local",
						eTag:            `"etag-value"`,
						maxAge:          60 * time.Second,
					},
				},
				{
					delay: 10 * time.Minute,
					responses: []*http.Response{
						newResponse(t, 500, map[string]any{}, "", ""),
						newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
					},
					expectedETag: `"etag-value"`,
					expectedConfig: AppProxyConfig{
						ID:              "5",
						Name:            "5",
						UpstreamAppHost: "app.local",
						eTag:            `"etag-value"`,
						maxAge:          60 * time.Second,
					},
				},
				{
					expectedETag: `"etag-value"`,
					expectedConfig: AppProxyConfig{
						ID:              "5",
						Name:            "5",
						UpstreamAppHost: "app.local",
						eTag:            `"etag-value"`,
						maxAge:          60 * time.Second,
					},
				},
				{
					delay: 31 * time.Second,
					responses: []*http.Response{
						newResponse(t, 304, map[string]any{}, `"etag-value"`, "max-age=30"),
					},
					expectedETag: `"etag-value"`,
					expectedConfig: AppProxyConfig{
						ID:              "5",
						Name:            "5",
						UpstreamAppHost: "app.local",
						eTag:            `"etag-value"`,
						maxAge:          60 * time.Second,
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
						newResponse(t, 200, map[string]any{"id": "6", "upstreamAppHost": "app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: AppProxyConfig{
						ID:              "6",
						Name:            "6",
						UpstreamAppHost: "app.local",
						eTag:            `"etag-value"`,
						maxAge:          60 * time.Second,
					},
				},
				{
					delay: 10 * time.Minute,
					responses: []*http.Response{
						newResponse(t, 200, map[string]any{"id": "6", "upstreamAppHost": "new-app.local"}, `"etag-new-value"`, "max-age=60"),
					},
					expectedETag: `"etag-value"`,
					expectedConfig: AppProxyConfig{
						ID:              "6",
						Name:            "6",
						UpstreamAppHost: "new-app.local",
						eTag:            `"etag-new-value"`,
						maxAge:          60 * time.Second,
					},
				},
				{
					expectedConfig: AppProxyConfig{
						ID:              "6",
						Name:            "6",
						UpstreamAppHost: "new-app.local",
						eTag:            `"etag-new-value"`,
						maxAge:          60 * time.Second,
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
						newResponse(t, 200, map[string]any{"id": "7", "upstreamAppHost": "app.local"}, `"etag-value"`, "max-age=60"),
					},
					expectedConfig: AppProxyConfig{
						ID:              "7",
						Name:            "7",
						UpstreamAppHost: "app.local",
						eTag:            `"etag-value"`,
						maxAge:          60 * time.Second,
					},
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
					expectedETag: `"etag-value"`,
					expectedConfig: AppProxyConfig{
						ID:              "7",
						Name:            "7",
						UpstreamAppHost: "app.local",
						eTag:            `"etag-value"`,
						maxAge:          60 * time.Second,
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
					expectedETag:      `"etag-value"`,
					expectedErrorCode: 500,
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

			loader := NewSandboxesAPILoader(log.NewDebugLogger(), clk, url, "").(*sandboxesAPILoader)
			loader.sender = loader.sender.(client.Client).WithTransport(transport)

			for _, attempt := range tc.attempts {
				transport.Reset()

				clk.Add(attempt.delay)

				if len(attempt.responses) > 0 {
					responder := httpmock.ResponderFromMultipleResponses(attempt.responses)

					transport.RegisterResponder(
						http.MethodGet,
						fmt.Sprintf("%s/apps/%s/proxy-config", url, tc.appID),
						func(req *http.Request) (*http.Response, error) {
							assert.Equal(t, attempt.expectedETag, req.Header.Get("If-None-Match"))

							return responder(req)
						},
					)
				}

				config, err := loader.LoadConfig(context.Background(), tc.appID)
				if attempt.expectedErrorCode != 0 {
					require.Error(t, err)
					var sandboxesError *SandboxesError
					errors.As(err, &sandboxesError)
					assert.Equal(t, attempt.expectedErrorCode, sandboxesError.StatusCode())
				} else {
					require.NoError(t, err)
					assert.Equal(t, attempt.expectedConfig, config)
				}

				assert.Equal(t, len(attempt.responses), transport.GetTotalCallCount())
			}
		})
	}
}

func newResponse(t *testing.T, code int, body map[string]any, eTag string, cacheControl string) *http.Response {
	t.Helper()

	response, err := httpmock.NewJsonResponse(code, body)
	require.NoError(t, err)
	response.Header.Set("ETag", eTag)
	response.Header.Set("Cache-Control", cacheControl)
	return response
}
