package api

import (
	"context"
	"fmt"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"testing"
)

func TestNewClient(t *testing.T) {
	logger, _ := utils.NewDebugLogger()
	c := NewClient(context.Background(), logger, false)
	assert.NotNil(t, c)
}

func TestSimpleRequest(t *testing.T) {
	logger, out := utils.NewDebugLogger()
	c := NewClient(context.Background(), logger, false)

	// Enable http mock
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.ActivateNonDefault(c.http.GetClient())

	// Get
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))
	res, err := c.R().Get("https://example.com")
	assert.NoError(t, err)
	assert.Equal(t, "test", res.String())
	assert.NoError(t, out.Flush())
	expected := "DEBUG  HTTP\tGET https://example.com | 200 | %s"
	utils.AssertWildcards(t, expected, out.Buffer.String(), "Unexpected log")
}

func TestRetry(t *testing.T) {
	logger, out := utils.NewDebugLogger()
	c := NewClient(context.Background(), logger, false)

	// Enable http mock
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.ActivateNonDefault(c.http.GetClient())

	// Get
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(504, `test`))
	res, err := c.R().Get("https://example.com")
	assert.NoError(t, err)
	assert.Equal(t, "test", res.String())
	assert.NoError(t, out.Flush())

	// Retries are logged
	assert.Greater(t, c.http.RetryCount, 2)
	for i := 0; i < c.http.RetryCount; i++ {
		expected := fmt.Sprintf("DEBUG  HTTP-WARN\tGET https://example.com | 504 | %%s | Retrying %dx ...", i)
		utils.AssertWildcards(t, expected, out.Buffer.String(), "Unexpected log")
	}
}

func TestVerboseHideSecret(t *testing.T) {
	logger, out := utils.NewDebugLogger()
	c := NewClient(context.Background(), logger, true)

	// Enable http mock
	httpmock.Activate()
	defer httpmock.DeactivateAndReset()
	httpmock.ActivateNonDefault(c.http.GetClient())

	// Get
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))
	res, err := c.R().SetHeader("X-StorageApi-Token", "my-token").Get("https://example.com")
	assert.NoError(t, err)
	assert.Equal(t, "test", res.String())
	assert.NoError(t, out.Flush())

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
`
	utils.AssertWildcards(t, expectedLog, out.Buffer.String(), "Unexpected log")
}
