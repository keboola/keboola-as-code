package http

import (
	"context"
	"fmt"
	"net/http"

	"github.com/go-resty/resty/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

// Client - http client.
type Client struct {
	ctx              context.Context
	resty            *resty.Client // wrapped http client
	baseUrl          string
	userAgent        string
	headers          map[string]string
	logger           log.Logger
	verbose          bool
	transport        http.RoundTripper
	retry            *RetryConfig
	requestIdCounter *utils.SafeCounter // each request has unique ID -> for logs
	poolIdCounter    *utils.SafeCounter // each pool has unique ID -> for logs
}

type ErrorWithResponse interface {
	error
	SetResponse(response *resty.Response)
}

type Option func(c *Client)

func WithBaseUrl(baseUrl string) Option {
	return func(c *Client) {
		c.baseUrl = baseUrl
	}
}

func WithUserAgent(v string) Option {
	return func(c *Client) {
		c.userAgent = v
	}
}

func WithHeader(key, value string) Option {
	return func(c *Client) {
		c.headers[key] = value
	}
}

func WithHeaders(headers map[string]string) Option {
	return func(c *Client) {
		for k, v := range headers {
			c.headers[k] = v
		}
	}
}

func WithLogger(logger log.Logger) Option {
	return func(c *Client) {
		c.logger = logger
	}
}

func WithVerbose(verbose bool) Option {
	return func(c *Client) {
		c.verbose = verbose
	}
}

func WithTransport(transport http.RoundTripper) Option {
	return func(c *Client) {
		c.transport = transport
	}
}

func WithRetry(retry RetryConfig) Option {
	return func(c *Client) {
		c.retry = &retry
	}
}

type contextKey string

func New(ctx context.Context, options ...Option) *Client {
	client := &Client{
		ctx:              ctx,
		headers:          make(map[string]string),
		requestIdCounter: utils.NewSafeCounter(0),
		poolIdCounter:    utils.NewSafeCounter(0),
	}

	// Apply options
	for _, o := range options {
		o(client)
	}

	// Default logger
	if client.logger == nil {
		client.logger = log.NewNopLogger()
	}

	// Default transport
	if client.transport == nil {
		client.transport = DefaultTransport()
	}

	// Default retry
	if client.retry == nil {
		v := DefaultRetry()
		client.retry = &v
	}

	// Default user agent
	if client.userAgent == "" {
		client.userAgent = fmt.Sprintf("keboola-cli/%s", build.BuildVersion)
	}

	client.resty = createHttpClient(&httpLogger{client: client}, client.verbose, client.transport, *client.retry)
	client.resty.SetBaseURL(client.baseUrl)
	client.resty.SetHeader("User-Agent", client.userAgent)
	client.resty.SetHeaders(client.headers)
	return client
}

func (c *Client) BaseUrl() string {
	return c.resty.BaseURL
}

func (c *Client) Header() http.Header {
	return c.resty.Header
}

func (c *Client) Request() *resty.Request {
	return c.resty.R().SetContext(c.ctx)
}

func createHttpClient(logger *httpLogger, verbose bool, transport http.RoundTripper, retry RetryConfig) *resty.Client {
	r := resty.New()
	r.SetLogger(logger)
	r.SetTransport(transport)
	r.SetTimeout(retry.TotalRequestTimeout)
	r.SetRetryCount(retry.Count)
	r.SetRetryWaitTime(retry.WaitTime)
	r.SetRetryMaxWaitTime(RetryWaitTimeMax)
	r.AddRetryCondition(retry.Condition)
	r.SetDebugBodyLimit(32 * 1024)
	r.SetDebug(verbose)

	// Log each request when done
	r.OnAfterResponse(func(c *resty.Client, res *resty.Response) error {
		req := res.Request
		msg := responseToLog(res)
		if res.IsSuccess() {
			// Log success
			logger.Debugf("%s", msg)
		}

		// Return error if present
		err := res.Error()
		if err != nil {
			// Set response to error if supported
			if v, ok := err.(ErrorWithResponse); ok {
				v.SetResponse(res)
			}

			// Return err, wrap if needed
			if v, ok := err.(error); ok {
				return v
			} else {
				return fmt.Errorf("%s | error: \"%s\"", urlForLog(req), err)
			}
		}

		// Return error if request failed
		if res.IsError() {
			return fmt.Errorf(`%s %s | returned http code %d`, req.Method, urlForLog(req), res.StatusCode())
		}

		return nil
	})

	return r
}
