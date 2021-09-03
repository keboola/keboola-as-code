package client

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
	"go.uber.org/zap"

	"keboola-as-code/src/build"
	"keboola-as-code/src/utils"
)

const (
	RequestTimeout        = 30 * time.Second
	HttpTimeout           = 30 * time.Second
	IdleConnTimeout       = 30 * time.Second
	TLSHandshakeTimeout   = 10 * time.Second
	ResponseHeaderTimeout = 20 * time.Second
	ExpectContinueTimeout = 2 * time.Second
	KeepAlive             = 20 * time.Second
	MaxIdleConns          = 32
	RetryCount            = 5
	RetryWaitTime         = 100 * time.Millisecond
	RetryWaitTimeMax      = 3 * time.Second
)

// Client - http client.
type Client struct {
	parentCtx        context.Context // context for parallel execution
	logger           *Logger
	resty            *resty.Client      // wrapped http client
	requestIdCounter *utils.SafeCounter // each request has unique ID -> for logs
	poolIdCounter    *utils.SafeCounter // each pool has unique ID -> for logs
}

type contextKey string

func NewClient(ctx context.Context, logger *zap.SugaredLogger, verbose bool) *Client {
	client := &Client{}
	client.logger = &Logger{logger}
	client.parentCtx = ctx
	client.resty = createHttpClient(client.logger)
	client.requestIdCounter = utils.NewSafeCounter(0)
	client.poolIdCounter = utils.NewSafeCounter(0)
	setupLogs(client, verbose)
	return client
}

func (c Client) WithHostUrl(hostUrl string) *Client {
	c.resty.SetHostURL(hostUrl)
	return &c
}

func (c *Client) GetRestyClient() *resty.Client {
	return c.resty
}

func (c *Client) Send(request *Request) {
	// Sent
	request.lock.Lock()
	request.sent = true
	request.lock.Unlock()
	restyResponse, err := request.Request.Send()

	// Done
	request.lock.Lock()
	request.Response = NewResponse(request, restyResponse, err)
	request.done = true
	request.lock.Unlock()

	// Listeners
	request.invokeListeners()
}

func (c *Client) Request(request *Request) *Request {
	request.sender = c
	return request
}

func (c *Client) NewRequest(method string, url string) *Request {
	r := c.resty.R()
	r.Method = method
	r.URL = url
	request := NewRequest(c.requestIdCounter.IncAndGet(), c, r)
	request.SetContext(c.parentCtx)
	return request
}

func (c *Client) HostUrl() string {
	return c.resty.HostURL
}

func (c *Client) SetHeader(header, value string) *Client {
	c.resty.SetHeader(header, value)
	return c
}

func (c *Client) Header() http.Header {
	return c.resty.Header
}

func (c *Client) SetError(err interface{}) *Client {
	c.resty.SetError(err)
	return c
}

func (c *Client) SetRetry(count int, waitTime time.Duration, maxWaitTime time.Duration) {
	c.resty.RetryWaitTime = waitTime
	c.resty.RetryMaxWaitTime = maxWaitTime
	c.resty.RetryCount = count
}

func createHttpClient(logger *Logger) *resty.Client {
	r := resty.New()
	r.SetLogger(logger)
	r.SetHeader("User-Agent", fmt.Sprintf("keboola-as-code/%s", build.BuildVersion))
	r.SetTimeout(RequestTimeout)
	r.SetRetryCount(RetryCount)
	r.SetRetryWaitTime(RetryWaitTime)
	r.SetRetryMaxWaitTime(RetryWaitTimeMax)
	r.SetTransport(createTransport())
	r.AddRetryCondition(createRetry())
	return r
}

// createRetry - retry on defined network and HTTP errors.
func createRetry() func(response *resty.Response, err error) bool {
	return func(response *resty.Response, err error) bool {
		// On network errors - except hostname not found
		if err != nil && (response == nil || response.StatusCode() == 0) {
			switch {
			case
				strings.Contains(err.Error(), "No address associated with hostname"):
				return false
			default:
				return true
			}
		}

		// On HTTP status codes
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
	}
}

// createTransport with custom timeouts.
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
		TLSHandshakeTimeout:   TLSHandshakeTimeout,
		ResponseHeaderTimeout: ResponseHeaderTimeout,
		ExpectContinueTimeout: ExpectContinueTimeout,
		MaxIdleConnsPerHost:   MaxIdleConns,
	}
}

func setupLogs(client *Client, verbose bool) {
	// Debug full request and response if verbose = true
	// Secrets are hidden see Logger
	if verbose {
		client.resty.SetDebug(true)
		client.resty.SetDebugBodyLimit(32 * 1024)
	}

	// Log each request when done
	client.resty.OnAfterResponse(func(c *resty.Client, res *resty.Response) error {
		req := res.Request
		msg := responseToLog(res)
		if res.IsSuccess() {
			// Log success
			client.logger.Debugf("%s", msg)
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
}

func responseToLog(res *resty.Response) string {
	req := res.Request
	return fmt.Sprintf("%s %s | %d | %s", req.Method, urlForLog(req), res.StatusCode(), res.Time())
}

func urlForLog(request *resty.Request) string {
	url := request.URL

	// No response -> url contains placeholders
	if request.RawRequest == nil {
		if pathParams, ok := request.Context().Value(contextKey("pathParams")).(map[string]string); ok {
			for p, v := range pathParams {
				url = strings.ReplaceAll(url, "{"+p+"}", "{"+p+"=\""+v+"\"}")
			}
		}

		if queryParams, ok := request.Context().Value(contextKey("queryParams")).(map[string]string); ok {
			var queryPairs []string
			for p, v := range queryParams {
				queryPairs = append(queryPairs, fmt.Sprintf("%s=\"%s\"", p, v))
			}
			if len(queryPairs) > 0 {
				url += " | query: " + strings.Join(queryPairs, ", ")
			}
		}
	}

	return url
}
