package client

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
	client, logger, _ := getMockedClientAndLogs(t, false)
	pool := client.NewPool(logger, func(pool *Pool, response *PoolResponse) error {
		// nop
		return response.Error()
	})
	assert.NoError(t, pool.StartAndWait())
}

func TestSimple(t *testing.T) {
	client, logger, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	c := &utils.SafeCounter{}
	pool := client.NewPool(logger, func(pool *Pool, response *PoolResponse) error {
		c.Inc()
		return response.Error()
	})

	pool.Send(pool.Req(resty.MethodGet, "https://example.com"))

	assert.NoError(t, pool.StartAndWait())
	assert.Equal(t, 1, c.Value())
	assert.Equal(t, 1, httpmock.GetCallCountInfo()["GET https://example.com"])
}

func TestSubRequest(t *testing.T) {
	client, logger, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	c := &utils.SafeCounter{}
	pool := client.NewPool(logger, func(pool *Pool, response *PoolResponse) error {
		// Simulate sub-requests
		// If one request is processed, then start another, until count < 30
		if c.Inc(); c.Value() < 30 {
			pool.Send(pool.Req(resty.MethodGet, "https://example.com"))
		}

		return response.Error()
	})

	pool.Send(pool.Req(resty.MethodGet, "https://example.com"))

	assert.NoError(t, pool.StartAndWait())
	assert.Equal(t, 30, c.Value())
	assert.Equal(t, 30, httpmock.GetCallCountInfo()["GET https://example.com"])
}

func TestProcessorError(t *testing.T) {
	client, logger, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	c := &utils.SafeCounter{}
	pool := client.NewPool(logger, func(pool *Pool, response *PoolResponse) error {
		pool.Send(pool.Req(resty.MethodGet, "https://example.com"))
		if c.Inc(); c.Value() == 10 {
			return errors.New("some error in response processor")
		}

		return response.Error()
	})

	pool.Send(pool.Req(resty.MethodGet, "https://example.com"))

	assert.Equal(t, errors.New("some error in response processor"), pool.StartAndWait())
	assert.GreaterOrEqual(t, c.Value(), 10)
	assert.GreaterOrEqual(t, httpmock.GetCallCountInfo()["GET https://example.com"], 10)
}

func TestNetworkError(t *testing.T) {
	client, logger, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))
	httpmock.RegisterResponder("GET", `https://example.com/error`, httpmock.NewErrorResponder(errors.New("network error")))

	c := &utils.SafeCounter{}
	pool := client.NewPool(logger, func(pool *Pool, response *PoolResponse) error {
		if c.Inc(); c.Value() == 10 {
			pool.Send(pool.Req(resty.MethodGet, "https://example.com/error"))
		} else {
			pool.Send(pool.Req(resty.MethodGet, "https://example.com"))
		}
		return response.Error()
	})

	pool.Send(pool.Req(resty.MethodGet, "https://example.com"))

	assert.Equal(t, errors.New("network error"), pool.StartAndWait().(*url.Error).Unwrap())
	assert.GreaterOrEqual(t, c.Value(), 10)
	assert.GreaterOrEqual(t, httpmock.GetCallCountInfo()["GET https://example.com"], 10)
}
