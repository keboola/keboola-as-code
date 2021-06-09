package api

import (
	"errors"
	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/utils"
	"net/url"
	"testing"
)

func TestEmpty(t *testing.T) {
	client, _ := getMockedClientAndLogs(t, false)
	pool := client.NewPool(func(pool *Pool, response *resty.Response) error {
		// nop
		return nil
	})
	pool.Start()
	assert.NoError(t, pool.Wait())
}

func TestSimple(t *testing.T) {
	client, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	c := &utils.SafeCounter{}
	pool := client.NewPool(func(pool *Pool, response *resty.Response) error {
		c.Inc()
		return nil
	})

	pool.Add(pool.R(resty.MethodGet, "https://example.com"))
	pool.Start()

	assert.NoError(t, pool.Wait())
	assert.Equal(t, 1, c.Value())
	assert.Equal(t, 1, httpmock.GetCallCountInfo()["GET https://example.com"])
}

func TestSubRequest(t *testing.T) {
	client, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	c := &utils.SafeCounter{}
	pool := client.NewPool(func(pool *Pool, response *resty.Response) error {
		// Simulate sub-requests
		// If one request is processed, then start another, until count < 30
		if c.Inc(); c.Value() < 30 {
			pool.Add(pool.R(resty.MethodGet, "https://example.com"))
		}

		return nil
	})

	pool.Add(pool.R(resty.MethodGet, "https://example.com"))
	pool.Start()

	assert.NoError(t, pool.Wait())
	assert.Equal(t, 30, c.Value())
	assert.Equal(t, 30, httpmock.GetCallCountInfo()["GET https://example.com"])
}

func TestProcessorError(t *testing.T) {
	client, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	c := &utils.SafeCounter{}
	pool := client.NewPool(func(pool *Pool, response *resty.Response) error {
		if c.Inc(); c.Value() == 10 {
			return errors.New("some error in response processor")
		}

		pool.Add(pool.R(resty.MethodGet, "https://example.com"))
		return nil
	})

	pool.Add(pool.R(resty.MethodGet, "https://example.com"))
	pool.Start()

	assert.Equal(t, errors.New("some error in response processor"), pool.Wait())
	assert.Equal(t, 10, c.Value())
	assert.Equal(t, 10, httpmock.GetCallCountInfo()["GET https://example.com"])
}

func TestNetworkError(t *testing.T) {
	client, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))
	httpmock.RegisterResponder("GET", `https://example.com/error`, httpmock.NewErrorResponder(errors.New("network error")))

	c := &utils.SafeCounter{}
	pool := client.NewPool(func(pool *Pool, response *resty.Response) error {
		if c.Inc(); c.Value() == 10 {
			pool.Add(pool.R(resty.MethodGet, "https://example.com/error"))
		} else {
			pool.Add(pool.R(resty.MethodGet, "https://example.com"))
		}
		return nil
	})

	pool.Add(pool.R(resty.MethodGet, "https://example.com"))
	pool.Start()

	assert.Equal(t, errors.New("network error"), pool.Wait().(*url.Error).Unwrap())
	assert.Equal(t, 10, c.Value())
	assert.Equal(t, 10, httpmock.GetCallCountInfo()["GET https://example.com"])
}
