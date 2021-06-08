package api

import (
	"context"
	"fmt"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"testing"
	"time"
)

func TestNewClient(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	c := NewClient(context.Background(), logger, false)
	assert.NotNil(t, c)
}

func TestSimpleRequest(t *testing.T) {
	c, out := getMockedClientAndLogs(t, false)

	// Mocked response
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	// Get
	res, err := c.R().Get("https://example.com")
	assert.NoError(t, err)
	assert.Equal(t, "test", res.String())
	assert.NoError(t, out.Flush())
	expected := "DEBUG  HTTP\tGET https://example.com | 200 | %s"
	utils.AssertWildcards(t, expected, out.Buffer.String(), "Unexpected log")
}

func TestRetry(t *testing.T) {
	c, out := getMockedClientAndLogs(t, false)

	// Mocked response
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(504, `test`))

	// Get
	res, err := c.R().Get("https://example.com")
	assert.NoError(t, err) // no network error
	assert.Equal(t, "test", res.String())
	assert.NoError(t, out.Flush())
	logs := out.Buffer.String()

	// Check number of requests
	assert.Equal(t, 1+c.http.RetryCount, httpmock.GetCallCountInfo()["GET https://example.com"])

	// Retries are logged
	assert.Greater(t, c.http.RetryCount, 2)
	for i := 1; i <= c.http.RetryCount; i++ {
		expected := fmt.Sprintf("DEBUG  HTTP-WARN\tGET https://example.com | 504 | %%s | Retrying %dx ...", i)
		assert.Regexp(t, utils.WildcardToRegexp(expected), logs)
	}

	// Error is logged
	expected := fmt.Sprintf(
		"DEBUG  HTTP-ERROR\tGET https://example.com | 504 | %%s | Tried %dx",
		1+c.http.RetryCount,
	)
	assert.Regexp(t, utils.WildcardToRegexp(expected), logs)
}

func TestDoNotRetry(t *testing.T) {
	c, out := getMockedClientAndLogs(t, false)

	// Short time in tests
	c.http.RetryWaitTime = 1 * time.Millisecond
	c.http.RetryMaxWaitTime = 1 * time.Millisecond

	// Mocked response
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(404, `test`))

	// Get
	res, err := c.R().Get("https://example.com")
	assert.NoError(t, err) // no network error
	assert.Equal(t, "test", res.String())
	assert.NoError(t, out.Flush())
	logs := out.Buffer.String()

	// Only one request, HTTP code 404 is not retried
	assert.Equal(t, 1, httpmock.GetCallCountInfo()["GET https://example.com"])

	// No retry
	assert.NotContains(t, "Retrying", logs)

	// Error is logged
	expected := "DEBUG  HTTP-WARN\tGET https://example.com | 404 | %s\n"
	utils.AssertWildcards(t, expected, logs, "Unexpected log")
}

func TestVerboseHideSecret(t *testing.T) {
	c, out := getMockedClientAndLogs(t, true)

	// Mocked response
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	// Get
	res, err := c.R().SetHeader("X-StorageApi-Token", "my-token").Get("https://example.com")
	assert.NoError(t, err)
	assert.Equal(t, "test", res.String())
	assert.NoError(t, out.Flush())

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
	utils.AssertWildcards(t, expectedLog, out.Buffer.String(), "Unexpected log")
}

func getMockedClientAndLogs(t *testing.T, verbose bool) (*Client, *utils.Writer) {
	// Create
	logger, out := utils.NewDebugLogger()
	c := NewClient(context.Background(), logger, verbose)

	// Set short retry delay in tests
	c.http.RetryWaitTime = 1 * time.Millisecond
	c.http.RetryMaxWaitTime = 1 * time.Millisecond

	// Mocked http transport
	httpmock.Activate()
	httpmock.ActivateNonDefault(c.http.GetClient())
	t.Cleanup(func() {
		httpmock.DeactivateAndReset()
	})

	return c, out
}
