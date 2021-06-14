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
	pool := client.NewPool(logger)
	assert.NoError(t, pool.StartAndWait())
}

func TestSimple(t *testing.T) {
	client, logger, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	successCounter := utils.NewSafeCounter(0)
	responseCounter := utils.NewSafeCounter(0)
	pool := client.NewPool(logger)
	pool.Request(client.Request(resty.MethodGet, "https://example.com")).
		OnResponse(func(response *Response) *Response {
			responseCounter.Inc()
			return response
		}).
		OnSuccess(func(response *Response) *Response {
			successCounter.Inc()
			return response
		}).
		OnError(func(response *Response) *Response {
			assert.Fail(t, "error not expected")
			return response
		}).
		Send()

	assert.NoError(t, pool.StartAndWait())
	assert.Equal(t, 1, successCounter.Get())
	assert.Equal(t, 1, responseCounter.Get())
	assert.Equal(t, 1, httpmock.GetCallCountInfo()["GET https://example.com"])
}

func TestSubRequest(t *testing.T) {
	client, logger, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	successCounter := utils.NewSafeCounter(0)
	responseCounter := utils.NewSafeCounter(0)
	pool := client.NewPool(logger)
	onResponse := func(response *Response) *Response {
		responseCounter.Inc()
		return response
	}
	onError := func(response *Response) *Response {
		assert.Fail(t, "error not expected")
		return response
	}
	var onSuccess ResponseCallback
	onSuccess = func(response *Response) *Response {
		successCounter.Inc()
		if successCounter.Get() < 30 {
			// Send sub-request
			pool.Request(client.Request(resty.MethodGet, "https://example.com")).
				OnResponse(onResponse).
				OnSuccess(onSuccess).
				OnError(onError).
				Send()
		}
		return response
	}

	pool.Request(client.Request(resty.MethodGet, "https://example.com")).
		OnResponse(onResponse).
		OnSuccess(onSuccess).
		OnError(onError).
		Send()

	assert.NoError(t, pool.StartAndWait())
	assert.Equal(t, 30, successCounter.Get())
	assert.Equal(t, 30, responseCounter.Get())
	assert.Equal(t, 30, httpmock.GetCallCountInfo()["GET https://example.com"])
}

func TestErrorInCallback(t *testing.T) {
	client, logger, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	c := utils.NewSafeCounter(0)
	pool := client.NewPool(logger)
	var onSuccess ResponseCallback
	onSuccess = func(response *Response) *Response {
		pool.Request(client.Request(resty.MethodGet, "https://example.com")).
			OnSuccess(onSuccess).
			Send()

		if c.Inc(); c.Get() == 10 {
			return response.SetError(errors.New("some error in response listener"))
		}
		return response
	}
	pool.Request(client.Request(resty.MethodGet, "https://example.com")).
		OnSuccess(onSuccess).
		Send()

	assert.Equal(t, errors.New("some error in response listener"), pool.StartAndWait())
	assert.GreaterOrEqual(t, c.Get(), 10)
	assert.GreaterOrEqual(t, httpmock.GetCallCountInfo()["GET https://example.com"], 10)
}

func TestNetworkError(t *testing.T) {
	client, logger, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))
	httpmock.RegisterResponder("GET", `https://example.com/error`, httpmock.NewErrorResponder(errors.New("network error")))

	c := utils.NewSafeCounter(0)
	pool := client.NewPool(logger)
	var onSuccess ResponseCallback
	onSuccess = func(response *Response) *Response {
		if c.Inc(); c.Get() == 10 {
			pool.Request(client.Request(resty.MethodGet, "https://example.com/error")).
				OnSuccess(onSuccess).
				Send()
		} else {
			pool.Request(client.Request(resty.MethodGet, "https://example.com")).
				OnSuccess(onSuccess).
				Send()
		}
		return response
	}
	pool.Request(client.Request(resty.MethodGet, "https://example.com")).
		OnSuccess(onSuccess).
		Send()
	assert.Equal(t, errors.New("network error"), pool.StartAndWait().(*url.Error).Unwrap())
	assert.GreaterOrEqual(t, c.Get(), 10)
	assert.GreaterOrEqual(t, httpmock.GetCallCountInfo()["GET https://example.com"], 10)
}

func TestOnSuccess(t *testing.T) {
	client, logger, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))

	successCaught := false
	responseCaught := false
	pool := client.NewPool(logger)
	pool.Request(client.Request(resty.MethodGet, "https://example.com")).
		OnSuccess(func(response *Response) *Response {
			successCaught = true
			return response
		}).
		OnError(func(response *Response) *Response {
			assert.Fail(t, "error not expected")
			return response
		}).
		OnResponse(func(response *Response) *Response {
			responseCaught = true
			return response
		}).
		Send()

	err := pool.StartAndWait()
	assert.True(t, successCaught)
	assert.True(t, responseCaught)
	assert.NoError(t, err)
	assert.Equal(t, 1, httpmock.GetCallCountInfo()["GET https://example.com"])
}

func TestOnError(t *testing.T) {
	client, logger, _ := getMockedClientAndLogs(t, false)
	httpmock.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))
	httpmock.RegisterResponder("GET", `https://example.com/error`, httpmock.NewErrorResponder(errors.New("network error")))

	errorCaught := false
	responseCaught := false
	pool := client.NewPool(logger)
	pool.Request(client.Request(resty.MethodGet, "https://example.com")).
		OnSuccess(func(response *Response) *Response {
			pool.Request(client.Request(resty.MethodGet, "https://example.com/error")).
				OnSuccess(func(response *Response) *Response {
					assert.Fail(t, "error expected")
					return response

				}).
				OnError(func(response *Response) *Response {
					errorCaught = true
					return response
				}).
				OnResponse(func(response *Response) *Response {
					responseCaught = true
					return response
				}).
				Send()
			return response
		}).
		OnError(func(response *Response) *Response {
			assert.Fail(t, "error not expected")
			return response
		}).
		Send()

	err := pool.StartAndWait()
	assert.True(t, errorCaught)
	assert.True(t, responseCaught)
	assert.Equal(t, errors.New("network error"), err.(*url.Error).Unwrap())
	assert.Equal(t, 1, httpmock.GetCallCountInfo()["GET https://example.com"])
	assert.Equal(t, 1, httpmock.GetCallCountInfo()["GET https://example.com/error"])
}

func TestSendWasNotCalled(t *testing.T) {
	client, logger, _ := getMockedClientAndLogs(t, false)
	pool := client.NewPool(logger)
	pool.Request(client.Request(resty.MethodGet, "https://example.com"))
	assert.PanicsWithError(t, `request[1] GET "https://example.com" was not sent - Send() method was not called`, func() {
		pool.StartAndWait()
	})
}
