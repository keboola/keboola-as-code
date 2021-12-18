package client

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

func TestEmpty(t *testing.T) {
	t.Parallel()
	client, _, logger := getMockedClientAndLogs(t, false)
	pool := client.NewPool(logger)
	assert.NoError(t, pool.StartAndWait())
}

func TestSimple(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	successCounter := utils.NewSafeCounter(0)
	responseCounter := utils.NewSafeCounter(0)
	pool := client.NewPool(logger)
	pool.Request(client.NewRequest(resty.MethodGet, "https://example.com")).
		OnResponse(func(response *Response) {
			responseCounter.Inc()
		}).
		OnSuccess(func(response *Response) {
			successCounter.Inc()
		}).
		OnError(func(response *Response) {
			assert.Fail(t, "error not expected")
		}).
		Send()

	assert.NoError(t, pool.StartAndWait())
	assert.Equal(t, 1, successCounter.Get())
	assert.Equal(t, 1, responseCounter.Get())
	assert.Equal(t, 1, httpTransport.GetCallCountInfo()["GET https://example.com"])
}

func TestSubRequestDelayed(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	var invokeOrder []int
	pool := client.NewPool(logger)
	pool.Request(client.NewRequest(resty.MethodGet, "https://example.com")).
		OnSuccess(func(response *Response) {
			subRequest := pool.Request(client.NewRequest(resty.MethodGet, "https://example.com/sub"))
			subRequest.OnSuccess(func(response *Response) {
				invokeOrder = append(invokeOrder, 1)
			})
			response.WaitFor(subRequest)
			subRequest.Send()
			time.Sleep(10 * time.Millisecond)
		}).
		OnSuccess(func(response *Response) {
			time.Sleep(20 * time.Millisecond)
			invokeOrder = append(invokeOrder, 2)
		}).
		OnSuccess(func(response *Response) {
			invokeOrder = append(invokeOrder, 3)
		}).
		OnSuccess(func(response *Response) {
			invokeOrder = append(invokeOrder, 4)
		}).
		Send()

	assert.NoError(t, pool.StartAndWait())
	assert.Equal(t, []int{1, 2, 3, 4}, invokeOrder)
	assert.Equal(t, 1, httpTransport.GetCallCountInfo()["GET https://example.com"])
	assert.Equal(t, 1, httpTransport.GetCallCountInfo()["GET https://example.com/sub"])
}

func TestSubRequest(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	successCounter := utils.NewSafeCounter(0)
	responseCounter := utils.NewSafeCounter(0)
	pool := client.NewPool(logger)
	onResponse := func(*Response) {
		responseCounter.Inc()
	}
	onError := func(*Response) {
		assert.Fail(t, "error not expected")
	}
	var onSuccess ResponseCallback
	onSuccess = func(response *Response) {
		successCounter.Inc()
		if successCounter.Get() < 30 {
			// Send sub-request
			pool.Request(client.NewRequest(resty.MethodGet, "https://example.com")).
				OnResponse(onResponse).
				OnSuccess(onSuccess).
				OnError(onError).
				Send()
		}
	}

	pool.Request(client.NewRequest(resty.MethodGet, "https://example.com")).
		OnResponse(onResponse).
		OnSuccess(onSuccess).
		OnError(onError).
		Send()

	assert.NoError(t, pool.StartAndWait())
	assert.Equal(t, 30, successCounter.Get())
	assert.Equal(t, 30, responseCounter.Get())
	assert.Equal(t, 30, httpTransport.GetCallCountInfo()["GET https://example.com"])
}

func TestErrorInCallback(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder("GET", `=~.+`, httpmock.NewStringResponder(200, `test`))

	c := utils.NewSafeCounter(0)
	pool := client.NewPool(logger)
	var onSuccess ResponseCallback
	onSuccess = func(response *Response) {
		pool.Request(client.NewRequest(resty.MethodGet, "https://example.com")).
			OnSuccess(onSuccess).
			Send()

		if c.Inc(); c.Get() == 10 {
			response.SetErr(errors.New("some error in response listener"))
		}
	}
	pool.Request(client.NewRequest(resty.MethodGet, "https://example.com")).
		OnSuccess(onSuccess).
		Send()

	assert.Equal(t, errors.New("some error in response listener"), pool.StartAndWait())
	assert.GreaterOrEqual(t, c.Get(), 10)
	assert.GreaterOrEqual(t, httpTransport.GetCallCountInfo()["GET https://example.com"], 10)
}

func TestNetworkError(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))
	httpTransport.RegisterResponder("GET", `https://example.com/error`, httpmock.NewErrorResponder(errors.New("network error")))

	c := utils.NewSafeCounter(0)
	pool := client.NewPool(logger)
	var onSuccess ResponseCallback
	onSuccess = func(response *Response) {
		if c.Inc(); c.Get() == 10 {
			pool.Request(client.NewRequest(resty.MethodGet, "https://example.com/error")).
				OnSuccess(onSuccess).
				Send()
		} else {
			pool.Request(client.NewRequest(resty.MethodGet, "https://example.com")).
				OnSuccess(onSuccess).
				Send()
		}
	}
	pool.Request(client.NewRequest(resty.MethodGet, "https://example.com")).
		OnSuccess(onSuccess).
		Send()
	assert.Contains(t, pool.StartAndWait().Error(), "network error")
	assert.GreaterOrEqual(t, c.Get(), 10)
	assert.GreaterOrEqual(t, httpTransport.GetCallCountInfo()["GET https://example.com"], 10)
}

func TestErrorInSubRequest(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))
	httpTransport.RegisterResponder("GET", `https://example.com/error`, httpmock.NewErrorResponder(errors.New("network error")))

	c := utils.NewSafeCounter(0)
	pool := client.NewPool(logger)
	var onSuccess ResponseCallback
	onSuccess = func(response *Response) {
		url := "https://example.com"
		if c.IncAndGet() == 10 {
			url = "https://example.com/error"
		}
		subRequest := pool.Request(client.NewRequest(resty.MethodGet, url)).OnSuccess(onSuccess)
		response.Request.WaitFor(subRequest)
		subRequest.Send()
	}

	mainRequest := pool.Request(client.NewRequest(resty.MethodGet, "https://example.com")).OnSuccess(onSuccess).Send()

	// Error is returned by pool
	err := pool.StartAndWait()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "network error")

	// Error is also set to the main request
	assert.True(t, mainRequest.HasError())
	assert.Contains(t, mainRequest.Err().Error(), "network error")

	assert.GreaterOrEqual(t, c.Get(), 10)
	assert.GreaterOrEqual(t, httpTransport.GetCallCountInfo()["GET https://example.com"], 10)
}

func TestOnSuccess(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))

	successCaught := false
	responseCaught := false
	pool := client.NewPool(logger)
	pool.Request(client.NewRequest(resty.MethodGet, "https://example.com")).
		OnSuccess(func(response *Response) {
			successCaught = true
		}).
		OnError(func(response *Response) {
			assert.Fail(t, "error not expected")
		}).
		OnResponse(func(response *Response) {
			responseCaught = true
		}).
		Send()

	assert.NoError(t, pool.StartAndWait())
	assert.True(t, successCaught)
	assert.True(t, responseCaught)
	assert.Equal(t, 1, httpTransport.GetCallCountInfo()["GET https://example.com"])
}

func TestOnError(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))
	httpTransport.RegisterResponder("GET", `https://example.com/error`, httpmock.NewErrorResponder(errors.New("network error")))

	errorCaught := false
	responseCaught := false
	pool := client.NewPool(logger)
	pool.Request(client.NewRequest(resty.MethodGet, "https://example.com")).
		OnSuccess(func(response *Response) {
			pool.Request(client.NewRequest(resty.MethodGet, "https://example.com/error")).
				OnSuccess(func(response *Response) {
					assert.Fail(t, "error expected")
				}).
				OnError(func(response *Response) {
					errorCaught = true
				}).
				OnResponse(func(response *Response) {
					responseCaught = true
				}).
				Send()
		}).
		OnError(func(response *Response) {
			assert.Fail(t, "error not expected")
		}).
		Send()

	err := pool.StartAndWait()
	assert.True(t, errorCaught)
	assert.True(t, responseCaught)
	assert.Contains(t, err.Error(), "network error")
	assert.Equal(t, 1, httpTransport.GetCallCountInfo()["GET https://example.com"])
	assert.Equal(t, 1+RetryCount, httpTransport.GetCallCountInfo()["GET https://example.com/error"])
}

func TestSendWasNotCalled(t *testing.T) {
	t.Parallel()
	client, _, logger := getMockedClientAndLogs(t, false)

	pool := client.NewPool(logger)
	pool.Request(client.NewRequest(resty.MethodGet, "https://example.com"))
	assert.PanicsWithError(t, `request[1] GET "https://example.com" was not sent - Send() method was not called`, func() {
		pool.StartAndWait()
	})
}

func TestWaitForSubRequest(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))
	httpTransport.RegisterResponder("GET", `https://example.com/sub`, httpmock.NewStringResponder(200, `test`))

	counter := utils.NewSafeCounter(0)

	var mainRequest *Request
	var subRequestCallback ResponseCallback
	pool := client.NewPool(logger)
	subRequestCallback = func(response *Response) {
		if counter.IncAndGet() <= 10 {
			// Send sub-request
			subRequest := pool.
				Request(client.NewRequest(resty.MethodGet, "https://example.com/sub")).
				OnSuccess(subRequestCallback)
			mainRequest.WaitFor(subRequest) // <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
			subRequest.Send()
		}
	}

	mainDoneCallbackCalled := false
	allDoneCallback1Called := false
	allDoneCallback2Called := false
	mainRequest = pool.
		Request(client.NewRequest(resty.MethodGet, "https://example.com")).
		OnSuccess(func(response *Response) {
			// Should be called as soon as the main request is done
			mainDoneCallbackCalled = true
			assert.Equal(t, 0, counter.Get())
		}).
		OnSuccess(subRequestCallback).
		OnSuccess(func(response *Response) {
			// Should be called when all sub-requests are done
			allDoneCallback1Called = true
			assert.Equal(t, 11, counter.Get())
		}).
		OnSuccess(func(response *Response) {
			// Should be called when all sub-requests are done
			allDoneCallback2Called = true
			assert.Equal(t, 11, counter.Get())
		}).
		Send()

	// No error, all callbacks was called, see asserts in callbacks
	assert.NoError(t, pool.StartAndWait())
	assert.True(t, mainDoneCallbackCalled)
	assert.True(t, allDoneCallback1Called)
	assert.True(t, allDoneCallback2Called)

	// Assert requests count
	assert.Equal(t, 11, counter.Get())
	assert.Equal(t, 1, httpTransport.GetCallCountInfo()["GET https://example.com"])
	assert.Equal(t, 10, httpTransport.GetCallCountInfo()["GET https://example.com/sub"])
}

func TestWaitForSubRequestChain(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder("GET", `https://example.com`, httpmock.NewStringResponder(200, `test`))
	httpTransport.RegisterResponder("GET", `https://example.com/sub`, httpmock.NewStringResponder(200, `test`))

	var invokeOrder []int
	var subRequestCallback ResponseCallback
	counter := utils.NewSafeCounter(0)
	pool := client.NewPool(logger)
	subRequestCallback = func(response *Response) {
		if counter.IncAndGet() <= 10 {
			// Send sub-request
			subRequest := pool.
				Request(client.NewRequest(resty.MethodGet, "https://example.com/sub")).
				OnSuccess(subRequestCallback).
				OnSuccess(func(response *Response) {
					invokeOrder = append(invokeOrder, response.Id())
				})
			response.WaitFor(subRequest) // main WaitFor -> sub1 -> sub2 -> sub3 ...
			subRequest.Send()
		}
	}

	allDoneCallbackCalled := false
	pool.
		Request(client.NewRequest(resty.MethodGet, "https://example.com")).
		OnSuccess(subRequestCallback).
		OnSuccess(func(response *Response) {
			// Should be called when all sub-requests are done
			allDoneCallbackCalled = true
			assert.Equal(t, 11, counter.Get())
		}).
		Send()

	// No error, callback called
	assert.NoError(t, pool.StartAndWait())
	assert.True(t, allDoneCallbackCalled)

	// Assert requests count
	assert.Equal(t, 11, counter.Get())
	assert.Equal(t, 1, httpTransport.GetCallCountInfo()["GET https://example.com"])
	assert.Equal(t, 10, httpTransport.GetCallCountInfo()["GET https://example.com/sub"])

	// Earlier requests are waiting for the next one
	// ... so callbacks are performed in reverse order, "1" is main request "2-11" sub requests
	assert.Equal(t, []int{11, 10, 9, 8, 7, 6, 5, 4, 3, 2}, invokeOrder)
}

func TestPoolManyRequestsUnderLimit(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder(`GET`, `https://example.com`, httpmock.NewStringResponder(200, `test`))
	pool := client.NewPool(logger)

	count := REQUESTS_BUFFER_SIZE - 1
	for i := 0; i < count; i++ {
		pool.Send(client.NewRequest(`GET`, `https://example.com`))
	}

	assert.NoError(t, pool.StartAndWait())
	assert.Equal(t, count, httpTransport.GetCallCountInfo()["GET https://example.com"])
}

func TestPoolTooManyRequests(t *testing.T) {
	t.Parallel()
	client, httpTransport, logger := getMockedClientAndLogs(t, false)
	httpTransport.RegisterResponder(`GET`, `https://example.com`, httpmock.NewStringResponder(200, `test`))
	pool := client.NewPool(logger)

	// Pool can handle it ...
	for i := 0; i < REQUESTS_BUFFER_SIZE-1; i++ {
		pool.Send(client.NewRequest(`GET`, `https://example.com`))
	}

	// This is too much
	assert.PanicsWithError(t, fmt.Sprintf(`Too many (%d) queued reuests in HTTP pool.`, REQUESTS_BUFFER_SIZE), func() {
		pool.Send(client.NewRequest(`GET`, `https://example.com`))
	})
}
