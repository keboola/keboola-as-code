package http

import (
	"context"
	"errors"
	"fmt"
	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"testing"
	"time"
)

func TestNewHttpClient(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	c := NewHttpClient(context.Background(), logger, false)
	assert.NotNil(t, c)
}

func TestWithHostUrl(t *testing.T) {
	orgClient, _ := getMockedClientAndLogs(t, false)
	hostClient := orgClient.WithHostUrl("https://foo.bar")

	// Mocked response
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	// Must be cloned, not modified
	assert.NotSame(t, orgClient, hostClient)
	_, err := hostClient.Req(resty.MethodGet, "/baz").Send()
	assert.NoError(t, err)

	// Check request url
	assert.Equal(t, 1, httpmock.GetCallCountInfo()["GET https://foo.bar/baz"])
}

func TestSimpleRequest(t *testing.T) {
	c, out := getMockedClientAndLogs(t, false)

	// Mocked response
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	// Get
	res, err := c.Req(resty.MethodGet, "https://example.com").Send()
	assert.NoError(t, err)
	assert.Equal(t, "test", res.String())
	expected := "DEBUG  HTTP\tGET https://example.com | 200 | %s"
	utils.AssertWildcards(t, expected, out.String(), "Unexpected log")
}

func TestRetry(t *testing.T) {
	c, out := getMockedClientAndLogs(t, false)

	// Mocked response
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(504, `test`))

	// Get
	res, err := c.Req(resty.MethodGet, "https://example.com").Send()
	assert.Equal(t, errors.New(`GET "https://example.com" returned http code 504`), err)
	assert.Equal(t, "test", res.String())
	logs := out.String()

	// Check number of requests
	assert.Equal(t, 1+c.resty.RetryCount, httpmock.GetCallCountInfo()["GET https://example.com"])

	// Retries are logged
	assert.Greater(t, c.resty.RetryCount, 2)
	for i := 1; i <= c.resty.RetryCount; i++ {
		expected := fmt.Sprintf(`DEBUG  HTTP-ERROR	GET "https://example.com" returned http code 504, Attempt %d`, i)
		assert.Regexp(t, utils.WildcardToRegexp(expected), logs)
	}

	// Error is logged
	expected := fmt.Sprintf(
		`DEBUG  HTTP-ERROR	GET "https://example.com" returned http code 504, Attempt %d`,
		1+c.resty.RetryCount,
	)
	assert.Regexp(t, utils.WildcardToRegexp(expected), logs)
}

func TestDoNotRetry(t *testing.T) {
	c, out := getMockedClientAndLogs(t, false)

	// Mocked response
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(404, `test`))

	// Get
	res, err := c.Req(resty.MethodGet, "https://example.com").Send()
	assert.Equal(t, errors.New(`GET "https://example.com" returned http code 404`), err)
	assert.Equal(t, "test", res.String())
	logs := out.String()

	// Only one request, HTTP code 404 is not retried
	assert.Equal(t, 1, httpmock.GetCallCountInfo()["GET https://example.com"])

	// No retry
	assert.NotContains(t, "Attempt 2", logs)

	// Error is logged
	expected := "DEBUG  HTTP-ERROR\tGET \"https://example.com\" returned http code 404, Attempt 1\n"
	utils.AssertWildcards(t, expected, logs, "Unexpected log")
}

func TestVerboseHideSecret(t *testing.T) {
	c, out := getMockedClientAndLogs(t, true)

	// Mocked response
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	// Get
	res, err := c.Req(resty.MethodGet, "https://example.com").SetHeader("X-StorageApi-Token", "my-token").Send()
	assert.NoError(t, err)
	assert.Equal(t, "test", res.String())

	// Assert logs
	expectedLog :=
		`DEBUG  HTTP	
==============================================================================
~~~ REQUEST ~~~
GET  /  HTTP/1.1
HOST   : example.com
HEADERS:
	User-Agent: keboola-as-code/dev
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
	utils.AssertWildcards(t, expectedLog, out.String(), "Unexpected log")
}

func getMockedClientAndLogs(t *testing.T, verbose bool) (*Client, *utils.Writer) {
	// Create
	logger, out := utils.NewDebugLogger()
	c := NewHttpClient(context.Background(), logger, verbose)

	// Set short retry delay in tests
	c.resty.RetryWaitTime = 1 * time.Millisecond
	c.resty.RetryMaxWaitTime = 1 * time.Millisecond

	// Mocked resty transport
	httpmock.Activate()
	httpmock.ActivateNonDefault(c.resty.GetClient())
	t.Cleanup(func() {
		httpmock.DeactivateAndReset()
	})

	return c, out
}
