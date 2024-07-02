package httpsource_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

type TestCase struct {
	Name               string
	Method             string
	Path               string
	Headers            map[string]string
	Body               io.Reader
	ExpectedErr        string
	ExpectedStatusCode int
	ExpectedHeaders    map[string]string
	ExpectedBody       string
	ExpectedLogs       string
}

//nolint:tparallel // we want to run the subtests - requests sequentially and check the logs
func TestStart(t *testing.T) {
	t.Parallel()

	maxHeaderSize := 2000
	maxBodySize := 8000

	port := netutils.FreePortForTest(t)
	listenAddr := fmt.Sprintf("localhost:%d", port)
	url := fmt.Sprintf(`http://%s`, listenAddr)
	d, mock := dependencies.NewMockedServiceScopeWithConfig(t, func(cfg *config.Config) {
		cfg.Source.HTTP.Listen = fmt.Sprintf("0.0.0.0:%d", port)
		cfg.Source.HTTP.ReadBufferSize = datasize.ByteSize(maxHeaderSize) * datasize.B // ReadBufferSize is a limit for headers, not for the body
		cfg.Source.HTTP.MaxRequestBodySize = datasize.ByteSize(maxBodySize) * datasize.B
	})
	logger := mock.DebugLogger()

	// Start
	ctx := context.Background()
	require.NoError(t, stream.StartComponents(ctx, d, mock.TestConfig(), stream.ComponentHTTPSource))

	// Wait for the HTTP server
	require.NoError(t, netutils.WaitForHTTP(url, 10*time.Second))
	logger.AssertJSONMessages(t, `
{"level":"info","message":"starting HTTP source node","component":"http-source"}
{"level":"info","message":"started HTTP source on \"0.0.0.0:%d\"","component":"http-source"}
`)

	// Send testing requests
	sendTestRequests(t, ctx, logger, url, maxHeaderSize, maxBodySize)

	// Shutdown
	logger.Truncate()
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()
	logger.AssertJSONMessages(t, `
{"level":"info","message":"exiting (bye bye)"}
{"level":"info","message":"shutting down HTTP source at \"0.0.0.0:%d\"","component":"http-source"}
{"level":"info","message":"HTTP source shutdown finished","component":"http-source"}
{"level":"info","message":"closing volumes stream","component":"volume.repository"}
{"level":"info","message":"closed volumes stream","component":"volume.repository"}
{"level":"info","message":"received shutdown request","component":"distribution.mutex.provider"}
{"level":"info","message":"closing etcd session: context canceled","component":"distribution.mutex.provider.etcd.session"}
{"level":"info","message":"closed etcd session","component":"distribution.mutex.provider.etcd.session"}
{"level":"info","message":"shutdown done","component":"distribution.mutex.provider"}
{"level":"info","message":"closing etcd connection","component":"etcd.client"}
{"level":"info","message":"closed etcd connection","component":"etcd.client"}
{"level":"info","message":"exited"}
`)
}

func testCases(t *testing.T, maxHeaderSize, maxBodySize int) []TestCase {
	t.Helper()

	require.Less(t, maxHeaderSize, maxBodySize)

	return []TestCase{
		{
			Name:               "health check",
			Method:             http.MethodGet,
			Path:               "/health-check",
			ExpectedStatusCode: http.StatusOK,
			ExpectedBody:       "OK\n",
		},
		{
			Name:               "not found",
			Method:             http.MethodGet,
			Path:               "/foo",
			ExpectedStatusCode: http.StatusNotFound,
			ExpectedLogs:       `{"level":"info","message":"not found, please send data using POST /stream/<sourceID>/<secret>"}`,
			ExpectedBody: `{
  "statusCode": 404,
  "error": "stream.in.routeNotFound",
  "message": "Not found, please send data using POST /stream/\u003csourceID\u003e/\u003csecret\u003e"
}`,
		},
		{
			Name:               "stream input - OPTIONS",
			Method:             http.MethodOptions,
			Path:               "/stream/my-source/my-secret",
			ExpectedStatusCode: http.StatusOK,
			ExpectedHeaders: map[string]string{
				"Allow":          "OPTIONS, POST",
				"Content-Length": "0",
			},
		},
		{
			Name:               "stream input - POST - ok, maximum body size",
			Method:             http.MethodPost,
			Path:               "/stream/my-source/my-secret",
			Body:               strings.NewReader(strings.Repeat(".", maxBodySize)),
			ExpectedStatusCode: http.StatusOK,
			ExpectedBody:       "not implemented",
		},
		{
			Name:               "stream input - POST - over maximum header size",
			Method:             http.MethodPost,
			Path:               "/stream/my-source/my-secret",
			Headers:            map[string]string{"foo": strings.Repeat(".", maxHeaderSize+1)},
			ExpectedStatusCode: http.StatusRequestEntityTooLarge,
			ExpectedLogs:       `{"level":"info","message":"request header size is over the maximum \"2000B\"","error.type":"%s/errors.HeaderTooLargeError"}`,
			ExpectedBody: `{
  "statusCode": 413,
  "error": "stream.in.headerTooLarge",
  "message": "Request header size is over the maximum \"2000B\"."
}`,
		},
		{
			Name:               "stream input - POST - over maximum body size",
			Method:             http.MethodPost,
			Path:               "/stream/my-source/my-secret",
			Body:               strings.NewReader(strings.Repeat(".", maxBodySize+1)),
			ExpectedStatusCode: http.StatusRequestEntityTooLarge,
			ExpectedLogs:       `{"level":"info","message":"request body size is over the maximum \"8000B\"","error.type":"%s/errors.BodyTooLargeError"}`,
			ExpectedBody: `{
  "statusCode": 413,
  "error": "stream.in.bodyTooLarge",
  "message": "Request body size is over the maximum \"8000B\"."
}`,
		},
	}
}

func sendTestRequests(t *testing.T, ctx context.Context, logger log.DebugLogger, url string, maxHeaderSize, maxBodySize int) {
	t.Helper()

	for _, tc := range testCases(t, maxHeaderSize, maxBodySize) {
		t.Run(strhelper.NormalizeName(tc.Name), func(t *testing.T) {
			logger.Truncate()

			// Method, URL, body
			require.NotEmpty(t, tc.Method)
			req, err := http.NewRequestWithContext(ctx, tc.Method, url+tc.Path, tc.Body)
			require.NoError(t, err)

			// Headers
			for k, v := range tc.Headers {
				req.Header.Set(k, v)
			}

			// Error + logs
			resp, err := http.DefaultClient.Do(req)
			logger.AssertJSONMessages(t, tc.ExpectedLogs)
			if tc.ExpectedErr != "" {
				if assert.Error(t, err) {
					wildcards.Assert(t, tc.ExpectedErr, err.Error())
				}
				return
			}

			// Expected status code
			require.NoError(t, err)
			assert.Equal(t, tc.ExpectedStatusCode, resp.StatusCode)

			// Expected headers
			if len(tc.ExpectedHeaders) > 0 {
				actualHeaders := make(map[string]string)
				for k, v := range resp.Header {
					actualHeaders[k] = v[0]
				}
				assert.Equal(t, tc.ExpectedHeaders, actualHeaders)
			}

			// Response body
			respBody, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.NoError(t, resp.Body.Close())
			wildcards.Assert(t, tc.ExpectedBody, string(respBody))
		})
	}
}
