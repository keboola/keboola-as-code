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
	isRetying map[*resty.Request]bool
}

func NewClient(parentCtx context.Context, logger *zap.SugaredLogger, verbose bool) *Client {
	client := &Client{}
	client.logger = &ClientLogger{logger}
	client.parentCtx = parentCtx
	client.http = createHttpClient(client.logger, verbose)
	client.isRetying = make(map[*resty.Request]bool)
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
		// On network errors
		if err != nil {
			return true
		}

		// On status codes
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
	// Secrets are hidden see ClientLogger
	if verbose {
		client.http.SetDebug(true)
		client.http.SetDebugBodyLimit(2 * 1024)
	}

	// Log each retry
	client.http.AddRetryHook(func(response *resty.Response, err error) {
		if response.Request.Attempt <= client.http.RetryCount {
			// Log retry
			msg := responseToLog(response)
			client.logger.Warnf(fmt.Sprintf("%s | Retrying %dx ...", msg, response.Request.Attempt))

			// Mark request retrying
			client.isRetying[response.Request] = true
		}
	})

	// Log each request when done
	client.http.OnAfterResponse(func(c *resty.Client, res *resty.Response) error {
		req := res.Request
		msg := responseToLog(res)
		if res.IsSuccess() {
			// Log success
			client.logger.Debugf(msg)
		} else {
			// Log error after last retry
			isRetrying := client.isRetying[req]
			if !isRetrying || req.Attempt > client.http.RetryCount {
				if req.Attempt > 1 {
					msg = fmt.Sprintf("%s | Tried %dx", msg, req.Attempt)
				}

				if isRetrying {
					client.logger.Errorf(msg)
				} else {
					client.logger.Warnf(msg)
				}

				// Clear
				delete(client.isRetying, res.Request)
			}
		}

		return nil
	})
}

func responseToLog(res *resty.Response) string {
	req := res.Request
	return fmt.Sprintf("%s %s | %d | %s", req.Method, req.URL, res.StatusCode(), res.Time())
}
