package proxy_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/umisama/go-regexpcache"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	proxyDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/testutil"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ptr"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/server"
)

type portManager struct{}

func newZeroPortManager() server.PortManager {
	return &portManager{}
}

func (p portManager) GeneratePorts() {}

func (p portManager) GetFreePort() int {
	return 0
}

func TestAppProxyHandler(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	// Start app
	pm := newZeroPortManager()
	appServer := testutil.StartAppServer(t, pm)
	defer appServer.Close()

	// Start api
	appsAPI := testutil.StartDataAppsAPI(t, pm)
	defer appsAPI.Close()

	// Configure proxy
	cfg := config.New()
	cfg.API.PublicURL, _ = url.Parse("https://hub.keboola.local")
	cfg.SandboxesAPI.URL = appsAPI.URL
	cfg.CsrfTokenSalt = "abc"

	// Create dependencies
	d, mocked := proxyDependencies.NewMockedServiceScope(t, ctx, cfg, dependencies.WithRealHTTPClient())

	// Register apps
	appURL := testutil.AddAppDNSRecord(t, appServer, mocked.TestDNSServer())
	appsAPI.Register([]api.AppConfig{
		{
			ID:             "123",
			Name:           "public",
			AppSlug:        ptr.Ptr("PUBLIC"),
			ProjectID:      "456",
			UpstreamAppURL: appURL.String(),
			AuthRules: []api.Rule{
				{
					Type:         api.RulePathPrefix,
					Value:        "/",
					AuthRequired: ptr.Ptr(false),
				},
			},
		},
	})

	// Create proxy handler
	handler := proxy.NewHandler(ctx, d)

	// Get robots.txt
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "https://hub.keboola.local/robots.txt", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Disallow: /")

	// Get missing asset
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "https://hub.keboola.local/_proxy/assets/foo.bar", nil)
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)

	// Invalid host
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "https://public-123.foo.bar.local/path", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "Unexpected domain, missing application ID.")

	// Send logged request
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "https://public-123.hub.keboola.local/path", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "Hello, client", rec.Body.String())

	// Send ignored request
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "https://hub.keboola.local/health-check", nil)
	req.Header.Set("User-Agent", "my-user-agent")
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "OK\n", rec.Body.String())

	mocked.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"req https://hub.keboola.local/_proxy/assets/foo.bar status=404 %s","http.request_id":"%s","component":"http"}
{"level":"warn","message":"badRequest: unexpected domain, missing application ID %A","http.request_id":"%s"}
{"level":"info","message":"req https://public-123.foo.bar.local/path status=400 %s","http.request_id":"%s","component":"http"}
{"level":"info","message":"req https://public-123.hub.keboola.local/path status=200 %s","http.request_id":"%s","component":"http"}
`)

	actualMetricsJSON := mocked.TestTelemetry().MetricsJSONString(
		t,
		telemetry.WithMetricFilter(func(metric metricdata.Metrics) bool {
			return strings.HasPrefix(metric.Name, "keboola.")
		}),
		telemetry.WithDataPointSortKey(func(attrs attribute.Set) string {
			host, _ := attrs.Value("net.host.name")
			status, _ := attrs.Value("http.status_code")
			return fmt.Sprintf("%d:%s", status.AsInt64(), host.AsString())
		}),
	)

	// Remove dynamic ports
	actualMetricsJSON = regexpcache.MustCompile(`app.local:\d+`).ReplaceAllString(actualMetricsJSON, `app.local:<port>`)

	// Remove dynamic DataPoints[].Value
	actualMetricsJSON = regexpcache.MustCompile(`(?m)^          "Value": \d+\n`).ReplaceAllString(actualMetricsJSON, ``)

	expectedMetricsJSON := `
[
  {
    "Name": "keboola.go.http.server.request.size",
    "Description": "Measures the size of HTTP request messages.",
    "Unit": "By",
    "Data": {
      "DataPoints": [
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 200
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "hub.keboola.local"
              }
            }
          ],
        },
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 200
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "public-123.hub.keboola.local"
              }
            },
            {
              "Key": "proxy.app.id",
              "Value": {
                "Type": "STRING",
                "Value": "123"
              }
            },
            {
              "Key": "proxy.app.name",
              "Value": {
                "Type": "STRING",
                "Value": "public"
              }
            },
            {
              "Key": "proxy.app.projectId",
              "Value": {
                "Type": "STRING",
                "Value": "456"
              }
            },
            {
              "Key": "proxy.app.upstream",
              "Value": {
                "Type": "STRING",
                "Value": "http://app.local:<port>"
              }
            }
          ],
        },
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 400
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "public-123.foo.bar.local"
              }
            }
          ],
        },
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 404
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "hub.keboola.local"
              }
            }
          ],
        }
      ],
      "Temporality": "CumulativeTemporality",
      "IsMonotonic": true
    }
  },
  {
    "Name": "keboola.go.http.server.response.size",
    "Description": "Measures the size of HTTP response messages.",
    "Unit": "By",
    "Data": {
      "DataPoints": [
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 200
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "hub.keboola.local"
              }
            }
          ],
        },
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 200
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "public-123.hub.keboola.local"
              }
            },
            {
              "Key": "proxy.app.id",
              "Value": {
                "Type": "STRING",
                "Value": "123"
              }
            },
            {
              "Key": "proxy.app.name",
              "Value": {
                "Type": "STRING",
                "Value": "public"
              }
            },
            {
              "Key": "proxy.app.projectId",
              "Value": {
                "Type": "STRING",
                "Value": "456"
              }
            },
            {
              "Key": "proxy.app.upstream",
              "Value": {
                "Type": "STRING",
                "Value": "http://app.local:<port>"
              }
            }
          ],
        },
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 400
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "public-123.foo.bar.local"
              }
            }
          ],
        },
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 404
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "hub.keboola.local"
              }
            }
          ],
        }
      ],
      "Temporality": "CumulativeTemporality",
      "IsMonotonic": true
    }
  },
  {
    "Name": "keboola.go.http.server.duration",
    "Description": "Measures the duration of inbound HTTP requests.",
    "Unit": "ms",
    "Data": {
      "DataPoints": [
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 200
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "hub.keboola.local"
              }
            }
          ],
          "Count": 2,
        },
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 200
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "public-123.hub.keboola.local"
              }
            },
            {
              "Key": "proxy.app.id",
              "Value": {
                "Type": "STRING",
                "Value": "123"
              }
            },
            {
              "Key": "proxy.app.name",
              "Value": {
                "Type": "STRING",
                "Value": "public"
              }
            },
            {
              "Key": "proxy.app.projectId",
              "Value": {
                "Type": "STRING",
                "Value": "456"
              }
            },
            {
              "Key": "proxy.app.upstream",
              "Value": {
                "Type": "STRING",
                "Value": "http://app.local:<port>"
              }
            }
          ],
          "Count": 1,
        },
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 400
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "public-123.foo.bar.local"
              }
            }
          ],
          "Count": 1,
        },
        {
          "Attributes": [
            {
              "Key": "http.method",
              "Value": {
                "Type": "STRING",
                "Value": "GET"
              }
            },
            {
              "Key": "http.scheme",
              "Value": {
                "Type": "STRING",
                "Value": "https"
              }
            },
            {
              "Key": "http.status_code",
              "Value": {
                "Type": "INT64",
                "Value": 404
              }
            },
            {
              "Key": "net.host.name",
              "Value": {
                "Type": "STRING",
                "Value": "hub.keboola.local"
              }
            }
          ],
          "Count": 1,
        }
      ],
      "Temporality": "CumulativeTemporality"
    }
  }
]
`
	assert.Equal(t, strings.TrimSpace(expectedMetricsJSON), strings.TrimSpace(actualMetricsJSON)) //nolint: testifylint
}
