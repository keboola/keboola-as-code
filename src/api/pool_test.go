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
	pool := client.NewPool(func(pool *Pool, response *PoolResponse) error {
		// nop
		return response.Error()
	})
	pool.Start()
	assert.NoError(t, pool.Wait())
}

func TestSimple(t *testing.T) {
	client, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	c := &utils.SafeCounter{}
	pool := client.NewPool(func(pool *Pool, response *PoolResponse) error {
		c.Inc()
		return response.Error()
	})

	pool.Send(pool.R(resty.MethodGet, "https://example.com"))
	pool.Start()

	assert.NoError(t, pool.Wait())
	assert.Equal(t, 1, c.Value())
	assert.Equal(t, 1, httpmock.GetCallCountInfo()["GET https://example.com"])
}

func TestSubRequest(t *testing.T) {
	client, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	c := &utils.SafeCounter{}
	pool := client.NewPool(func(pool *Pool, response *PoolResponse) error {
		// Simulate sub-requests
		// If one request is processed, then start another, until count < 30
		if c.Inc(); c.Value() < 30 {
			pool.Send(pool.R(resty.MethodGet, "https://example.com"))
		}

		return response.Error()
	})

	pool.Send(pool.R(resty.MethodGet, "https://example.com"))
	pool.Start()

	assert.NoError(t, pool.Wait())
	assert.Equal(t, 30, c.Value())
	assert.Equal(t, 30, httpmock.GetCallCountInfo()["GET https://example.com"])
}

func TestProcessorError(t *testing.T) {
	client, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	c := &utils.SafeCounter{}
	pool := client.NewPool(func(pool *Pool, response *PoolResponse) error {
		pool.Send(pool.R(resty.MethodGet, "https://example.com"))
		if c.Inc(); c.Value() == 10 {
			return errors.New("some error in response processor")
		}

		return response.Error()
	})

	pool.Send(pool.R(resty.MethodGet, "https://example.com"))
	pool.Start()

	assert.Equal(t, errors.New("some error in response processor"), pool.Wait())
	assert.GreaterOrEqual(t, c.Value(), 10)
	assert.GreaterOrEqual(t, httpmock.GetCallCountInfo()["GET https://example.com"], 10)
}

func TestNetworkError(t *testing.T) {
	client, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))
	httpmock.RegisterResponder("GET", `https://example.com/error`, httpmock.NewErrorResponder(errors.New("network error")))

	c := &utils.SafeCounter{}
	pool := client.NewPool(func(pool *Pool, response *PoolResponse) error {
		if c.Inc(); c.Value() == 10 {
			pool.Send(pool.R(resty.MethodGet, "https://example.com/error"))
		} else {
			pool.Send(pool.R(resty.MethodGet, "https://example.com"))
		}
		return response.Error()
	})

	pool.Send(pool.R(resty.MethodGet, "https://example.com"))
	pool.Start()

	assert.Equal(t, errors.New("network error"), pool.Wait().(*url.Error).Unwrap())
	assert.GreaterOrEqual(t, c.Value(), 10)
	assert.GreaterOrEqual(t, httpmock.GetCallCountInfo()["GET https://example.com"], 10)
}
