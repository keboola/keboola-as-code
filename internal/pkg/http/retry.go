package http

import (
	"net/http"
	"strings"
	"time"

	"github.com/go-resty/resty/v2"
)

const (
	RequestTimeout   = 30 * time.Second
	RetryCount       = 5
	RetryWaitTime    = 100 * time.Millisecond
	RetryWaitTimeMax = 3 * time.Second
)

type RetryConfig struct {
	TotalRequestTimeout time.Duration
	Condition           resty.RetryConditionFunc
	Count               int
	WaitTime            time.Duration
	WaitTimeMax         time.Duration
}

// TestingRetry - fast retry for use in tests.
func TestingRetry() RetryConfig {
	v := DefaultRetry()
	v.WaitTime = 1 * time.Millisecond
	v.WaitTimeMax = 1 * time.Millisecond
	return v
}

func DefaultRetry() RetryConfig {
	return RetryConfig{
		TotalRequestTimeout: RequestTimeout,
		Count:               RetryCount,
		WaitTime:            RetryWaitTime,
		WaitTimeMax:         RetryWaitTimeMax,
		Condition:           defaultRetryCondition(),
	}
}

// defaultRetryCondition - retry on defined network and HTTP errors.
func defaultRetryCondition() func(response *resty.Response, err error) bool {
	return func(response *resty.Response, err error) bool {
		// On network errors - except hostname not found
		if err != nil && (response == nil || response.StatusCode() == 0) {
			switch {
			case strings.Contains(err.Error(), "No address associated with hostname"):
				return false
			case strings.Contains(err.Error(), "no such host"):
				return false
			default:
				return true
			}
		}

		// On HTTP status codes
		switch response.StatusCode() {
		case
			http.StatusNotFound, // race condition
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
