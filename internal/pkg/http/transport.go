package http

import (
	"net"
	"net/http"
	"time"
)

const (
	HttpTimeout           = 30 * time.Second
	IdleConnTimeout       = 30 * time.Second
	TLSHandshakeTimeout   = 10 * time.Second
	ResponseHeaderTimeout = 20 * time.Second
	ExpectContinueTimeout = 2 * time.Second
	KeepAlive             = 20 * time.Second
	MaxIdleConns          = 32
)

// DefaultTransport with custom timeouts.
func DefaultTransport() *http.Transport {
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
