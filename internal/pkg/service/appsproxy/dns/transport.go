package dns

import (
	"context"
	"net"
	"net/http"
	"time"

	"golang.org/x/net/http2"
)

// TLSHandshakeTimeout specifies the default timeout of TLS handshake.
const TLSHandshakeTimeout = 5 * time.Second

// ResponseHeaderTimeout specifies the default amount of time to wait for a server's response headers.
const ResponseHeaderTimeout = 15 * time.Second

// MaxIdleConnections specifies the default maximum number of open connections at all.
const MaxIdleConnections = 128

// MaxConnectionsPerHost specifies the default maximum number of open connections to a host.
const MaxConnectionsPerHost = 32

// HTTP2ReadIdleTimeout is the timeout after which a health check using ping frame will be carried out.
const HTTP2ReadIdleTimeout = 2 * time.Second

// HTTP2PingTimeout is the timeout after which the connection will be closed  if a response to Ping is not received.
const HTTP2PingTimeout = 2 * time.Second

// HTTP2WriteByteTimeout is the timeout after which the connection will be closed no data can be written to it.
const HTTP2WriteByteTimeout = 15 * time.Second

// NewDNSSkippingHTTPTransport creates an HTTP transport which can skip DNS resolution if the address is already within request context.
// Timeouts are optimized for our usage in reverse proxy.
func NewDNSSkippingHTTPTransport(addressCtxKey any) (*http.Transport, error) {
	dialer := newDialer()

	transport := http.DefaultTransport.(*http.Transport).Clone()

	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		address, ok := ctx.Value(addressCtxKey).(string)
		if ok {
			addr = address
		}

		return dialer.DialContext(ctx, network, addr)
	}

	transport.DisableKeepAlives = false
	transport.TLSHandshakeTimeout = TLSHandshakeTimeout
	transport.ResponseHeaderTimeout = ResponseHeaderTimeout
	transport.MaxIdleConns = MaxIdleConnections
	transport.MaxConnsPerHost = MaxConnectionsPerHost
	transport.MaxIdleConnsPerHost = MaxConnectionsPerHost

	// Configure HTTP2
	http2Transport, err := http2.ConfigureTransports(transport)
	if err != nil {
		return nil, err
	}
	http2Transport.ReadIdleTimeout = HTTP2ReadIdleTimeout
	http2Transport.PingTimeout = HTTP2PingTimeout
	http2Transport.WriteByteTimeout = HTTP2WriteByteTimeout

	return transport, nil
}
