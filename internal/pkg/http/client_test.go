package http

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestNew(t *testing.T) {
	t.Parallel()
	c := New(context.Background())
	assert.NotNil(t, c)
}

func TestSimpleRequest(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, "test"))

	c := New(context.Background(), WithTransport(transport))
	_, err := c.Request().Get("https://foo.bar/baz")
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://foo.bar/baz"])
}

func TestWithBaseUrl(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, "test"))

	c := New(context.Background(), WithTransport(transport), WithBaseUrl("https://foo.bar"))
	_, err := c.Request().Get("/baz")
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://foo.bar/baz"])
}

func TestContext(t *testing.T) {
	t.Parallel()
	ctx := context.WithValue(context.Background(), "testKey", "testValue")

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `=~.+`, func(request *http.Request) (*http.Response, error) {
		// Client context should be used by the request
		assert.Equal(t, "testValue", request.Context().Value("testKey"))
		return httpmock.NewStringResponse(200, "test"), nil
	})

	c := New(ctx, WithTransport(transport))
	_, err := c.Request().Get("https://foo.bar/baz")
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://foo.bar/baz"])
}

func TestDefaultUserAgent(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `=~.+`, func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent": []string{"keboola-cli/dev"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	c := New(context.Background(), WithTransport(transport))
	_, err := c.Request().Get("https://foo.bar/baz")
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://foo.bar/baz"])
}

func TestCustomUserAgent(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `=~.+`, func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent": []string{"my-user-agent"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	c := New(context.Background(), WithTransport(transport), WithUserAgent("my-user-agent"))
	_, err := c.Request().Get("https://foo.bar/baz")
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://foo.bar/baz"])
}

func TestWithHeader(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `=~.+`, func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent": []string{"keboola-cli/dev"},
			"My-Header":  []string{"my-value"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	c := New(context.Background(), WithTransport(transport), WithHeader("my-header", "my-value"))
	_, err := c.Request().Get("https://foo.bar/baz")
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://foo.bar/baz"])
}

func TestWithHeaders(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `=~.+`, func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.Header{
			"User-Agent": []string{"keboola-cli/dev"},
			"Key1":       []string{"value1"},
			"Key2":       []string{"value2"},
		}, request.Header)
		return httpmock.NewStringResponse(200, "test"), nil
	})

	c := New(context.Background(), WithTransport(transport), WithHeaders(map[string]string{
		"key1": "value1",
		"key2": "value2",
	}))
	_, err := c.Request().Get("https://foo.bar/baz")
	assert.NoError(t, err)
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://foo.bar/baz"])
}

func TestRetry(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(504, "test"))

	// Create client
	logger := log.NewDebugLogger()
	c := New(context.Background(), WithTransport(transport), WithLogger(logger), WithRetry(TestingRetry()))

	// Get
	response, err := c.Request().Get("https://example.com")
	assert.Error(t, err)
	assert.Equal(t, `GET https://example.com | returned http code 504`, err.Error())
	assert.Equal(t, "test", response.String())
	logs := logger.AllMessages()

	// Check number of requests
	assert.Equal(t, 1+c.resty.RetryCount, transport.GetCallCountInfo()["GET https://example.com"])

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

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(403, "test"))

	// Create client
	logger := log.NewDebugLogger()
	c := New(context.Background(), WithTransport(transport), WithLogger(logger))

	// Get
	response, err := c.Request().Get("https://example.com")
	assert.Error(t, err)
	assert.Equal(t, `GET https://example.com | returned http code 403`, err.Error())
	assert.Equal(t, "test", response.String())
	logs := logger.AllMessages()

	// Only one request, HTTP code 403 is not retried
	assert.Equal(t, 1, transport.GetCallCountInfo()["GET https://example.com"])

	// No retry
	assert.NotContains(t, "Attempt 2", logs)

	// Error is logged
	expected := "DEBUG  HTTP-ERROR\tGET https://example.com | returned http code 403, Attempt 1\n"
	testhelper.AssertWildcards(t, expected, logs, "Unexpected log")
}

func TestVerboseHideSecret(t *testing.T) {
	t.Parallel()

	// Mocked response
	transport := httpmock.NewMockTransport()
	transport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, "test"))

	// Create client
	logger := log.NewDebugLogger()
	c := New(context.Background(), WithTransport(transport), WithLogger(logger), WithVerbose(true))

	// Get
	response, err := c.Request().SetHeader("X-StorageApi-Token", "123-my-token").Get("https://example.com")
	assert.NoError(t, err)
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
	testhelper.AssertWildcards(t, expectedLog, logger.AllMessages(), "Unexpected log")
}
