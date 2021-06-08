package api

import (
	"context"
	"fmt"
	resty "github.com/go-resty/resty/v2"
	"go.uber.org/zap"
	"keboola-as-code/src/version"
	"net"
	"net/http"
	"time"
)

const (
	RequestTimeout   = 45 * time.Second
	HttpTimeout      = 30 * time.Second
	IdleConnTimeout  = 90 * time.Second
	KeepAlive        = 30 * time.Second
	MaxIdleConns     = 64
	RetryCount       = 5
	RetryWaitTime    = 100 * time.Millisecond
	RetryWaitTimeMax = 3 * time.Second
)

type Client struct {
	parentCtx context.Context // context for parallel execution
	logger    *ClientLogger
	http      *resty.Client
	retries   map[*resty.Request]uint
}

func NewClient(parentCtx context.Context, logger *zap.SugaredLogger, verbose bool) *Client {
	client := &Client{}
	client.logger = &ClientLogger{logger}
	client.parentCtx = parentCtx
	client.http = createHttpClient(client.logger, verbose)
	client.retries = make(map[*resty.Request]uint)
	setupLogs(client, verbose)

	return client
}

func (c *Client) R() *resty.Request {
	return c.http.R()
}

func createHttpClient(logger *ClientLogger, verbose bool) *resty.Client {

	c := resty.New()
	c.SetLogger(logger)
	c.SetHeader("User-Agent", fmt.Sprintf("keboola-as-code/%s", version.BuildVersion))
	c.SetTimeout(RequestTimeout)
	c.SetTransport(createTransport())
	c.SetRetryCount(RetryCount)
	c.SetRetryWaitTime(RetryWaitTime)
	c.SetRetryMaxWaitTime(RetryWaitTimeMax)
	c.AddRetryCondition(func(response *resty.Response, err error) bool {
		switch response.StatusCode() {
		case
			http.StatusRequestTimeout,
			http.StatusConflict,
			http.StatusLocked,
			http.StatusTooManyRequests,
			http.StatusInternalServerError,
			http.StatusBadGateway,
			http.StatusServiceUnavailable,
			http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	})

	return c
}

func createTransport() *http.Transport {
	dialer := &net.Dialer{
		Timeout:   HttpTimeout,
		KeepAlive: KeepAlive,
	}
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          MaxIdleConns,
		IdleConnTimeout:       IdleConnTimeout,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		MaxIdleConnsPerHost:   MaxIdleConns,
	}
}

func setupLogs(client *Client, verbose bool) {
	// Debug full request and response if verbose = true
	if verbose {
		client.http.SetDebug(true)
		client.http.SetDebugBodyLimit(2 * 1024)
		return
	}

	// Log only simple message if verbose = false
	client.http.AddRetryHook(func(response *resty.Response, err error) {
		client.retries[response.Request]++
		attempt := client.retries[response.Request]
		if int(attempt) <= client.http.RetryCount {
			msg := responseToLog(response)
			client.logger.Warnf(fmt.Sprintf("%s | Retrying %dx ..", msg, attempt))
		}
	})
	client.http.OnAfterResponse(func(c *resty.Client, response *resty.Response) error {
		if response.IsSuccess() {
			client.logger.Debugf(responseToLog(response))
		}
		return nil
	})
	client.http.OnError(func(request *resty.Request, err error) {
		client.logger.Debugf("test")
		var msg string
		if v, ok := err.(*resty.ResponseError); ok {
			msg = responseToLog(v.Response)
		} else {
			msg = requestToLog(request, err)
		}

		attempt, retry := client.retries[request]
		if retry {
			msg = fmt.Sprintf("%s | Retried %dx", msg, attempt)
		}

		client.logger.Errorf(msg)
		delete(client.retries, request)
	})
}

func requestToLog(req *resty.Request, err error) string {
	return fmt.Sprintf("%s %s | %s", req.Method, req.URL, err)
}

func responseToLog(res *resty.Response) string {
	req := res.Request
	return fmt.Sprintf("%s %s | %d | %s", req.Method, req.URL, res.StatusCode(), res.Time())
}
