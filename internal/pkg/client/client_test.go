package client

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

func TestNewHttpClient(t *testing.T) {
	t.Parallel()
	logger := log.NewDebugLogger()
	c := NewClient(context.Background(), logger, false)
	assert.NotNil(t, c)
}

func TestWithHostUrl(t *testing.T) {
	t.Parallel()
	orgClient, httpTransport, _ := getMockedClientAndLogs(t, false)
	hostClient := orgClient.WithHostUrl("https://foo.bar")

	// Mocked response
	httpTransport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	// Must be cloned, not modified
	assert.NotSame(t, orgClient, hostClient)
	response := hostClient.NewRequest(resty.MethodGet, "/baz").Send().Response
	assert.NoError(t, response.Err())

	// Check request url
	assert.Equal(t, 1, httpTransport.GetCallCountInfo()["GET https://foo.bar/baz"])
}

func TestSimpleRequest(t *testing.T) {
	t.Parallel()
	c, httpTransport, logger := getMockedClientAndLogs(t, false)

	// Mocked response
	httpTransport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	// Get
	response := c.NewRequest(resty.MethodGet, "https://example.com").Send().Response
	assert.NoError(t, response.Err())
	assert.Equal(t, "test", response.String())
	expected := "DEBUG  HTTP\tGET https://example.com | 200 | %s"
	testhelper.AssertWildcards(t, expected, logger.AllMessages(), "Unexpected log")
}

func TestRetry(t *testing.T) {
	t.Parallel()
	c, httpTransport, logger := getMockedClientAndLogs(t, false)

	// Mocked response
	httpTransport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(504, `test`))

	// Get
	response := c.NewRequest(resty.MethodGet, "https://example.com").Send().Response
	assert.Equal(t, errors.New(`GET https://example.com | returned http code 504`), response.Err())
	assert.Equal(t, "test", response.String())
	logs := logger.AllMessages()

	// Check number of requests
	assert.Equal(t, 1+c.resty.RetryCount, httpTransport.GetCallCountInfo()["GET https://example.com"])

	// Retries are logged
	assert.Greater(t, c.resty.RetryCount, 2)
	for i := 1; i <= c.resty.RetryCount; i++ {
		expected := fmt.Sprintf(`DEBUG  HTTP-ERROR	GET https://example.com | returned http code 504, Attempt %d`, i)
		assert.Regexp(t, testhelper.WildcardToRegexp(expected), logs)
	}

	// Error is logged
	expected := fmt.Sprintf(
		`DEBUG  HTTP-ERROR	GET https://example.com | returned http code 504, Attempt %d`,
		1+c.resty.RetryCount,
	)
	assert.Regexp(t, testhelper.WildcardToRegexp(expected), logs)
}

func TestDoNotRetry(t *testing.T) {
	t.Parallel()
	c, httpTransport, logger := getMockedClientAndLogs(t, false)

	// Mocked response
	httpTransport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(403, `test`))

	// Get
	response := c.NewRequest(resty.MethodGet, "https://example.com").Send().Response
	assert.Equal(t, errors.New(`GET https://example.com | returned http code 403`), response.Err())
	assert.Equal(t, "test", response.String())
	logs := logger.AllMessages()

	// Only one request, HTTP code 403 is not retried
	assert.Equal(t, 1, httpTransport.GetCallCountInfo()["GET https://example.com"])

	// No retry
	assert.NotContains(t, "Attempt 2", logs)

	// Error is logged
	expected := "DEBUG  HTTP-ERROR\tGET https://example.com | returned http code 403, Attempt 1\n"
	testhelper.AssertWildcards(t, expected, logs, "Unexpected log")
}

func TestVerboseHideSecret(t *testing.T) {
	t.Parallel()
	c, httpTransport, out := getMockedClientAndLogs(t, true)

	// Mocked response
	httpTransport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	// Get
	response := c.NewRequest(resty.MethodGet, "https://example.com").SetHeader("X-StorageApi-Token", "123-my-token").Send().Response
	assert.NoError(t, response.Err())
	assert.Equal(t, "test", response.String())

	// Assert logs
	expectedLog :=
		`DEBUG  HTTP	
==============================================================================
~~~ REQUEST ~~~
GET  /  HTTP/1.1
HOST   : example.com
HEADERS:
	User-Agent: keboola-cli/dev
	X-Storageapi-Token: *****
BODY   :
***** NO CONTENT *****
------------------------------------------------------------------------------
~~~ RESPONSE ~~~
STATUS       : 200
PROTO        : 
RECEIVED AT  : %s
TIME DURATION: %s
HEADERS      :

BODY         :
test
==============================================================================
DEBUG  HTTP	GET https://example.com | 200 | %s

`
	testhelper.AssertWildcards(t, expectedLog, out.AllMessages(), "Unexpected log")
}

func getMockedClientAndLogs(t *testing.T, verbose bool) (*Client, *httpmock.MockTransport, log.DebugLogger) {
	t.Helper()

	// Create
	logger := log.NewDebugLogger()
	c := NewClient(context.Background(), logger, verbose)

	// Set short retry delay in tests
	c.resty.RetryWaitTime = 1 * time.Millisecond
	c.resty.RetryMaxWaitTime = 1 * time.Millisecond

	// Mocked resty transport
	transport := httpmock.NewMockTransport()
	c.resty.GetClient().Transport = transport
	return c, transport, logger
}
