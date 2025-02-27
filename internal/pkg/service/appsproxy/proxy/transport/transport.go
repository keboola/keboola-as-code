// Package transport provides HTTP transport for the reverse HTTP proxy with custom DNS resolving.
// DNS client is used to fast and reliable determine if the target data app is running.
package transport

import (
	"context"
	"net"
	"net/http"
	"net/http/httptrace"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/net/http2"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/transport/dns"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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

const DNSResolveTimeout = 5 * time.Second

type dependencies interface {
	Telemetry() telemetry.Telemetry
}

// New creates new http transport intended to be used with NewSingleHostReverseProxy.
// DNS server address is loaded from the "/etc/resolv.conf".
func New(d dependencies, dnsServer string) (http.RoundTripper, error) {
	return NewWithDNSServer(d, dnsServer)
}

// NewWithDNSServer creates new http transport intended to be used with NewSingleHostReverseProxy.
func NewWithDNSServer(d dependencies, dnsServerAddress string) (http.RoundTripper, error) {
	dialer := newDialer()

	var dnsClient *dns.Client
	var err error
	if dnsServerAddress == "" {
		dnsClient, err = dns.NewClientFromEtc(dialer)
		if err != nil {
			return nil, err
		}
	} else {
		dnsClient = dns.NewClient(dialer, dnsServerAddress)
	}

	tel := d.Telemetry()

	dialContext := func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}

		if net.ParseIP(host) != nil {
			return dialer.DialContext(ctx, network, address)
		}

		// Get tracing hooks
		trace := httptrace.ContextClientTrace(ctx)

		// Trace DNSStart
		ctx, dnsSpan := tel.Tracer().Start(ctx, "keboola.go.apps-proxy.transport.dns.resolve")
		dnsSpan.SetAttributes(
			attribute.String("transport.dns.resolve.host", host),
			attribute.String("transport.dns.resolve.server", dnsClient.DNSServer()),
		)
		if trace != nil && trace.DNSStart != nil {
			trace.DNSStart(httptrace.DNSStartInfo{Host: host})
		}

		// Create context for DNS resolving
		// It separates the events/tracing of the connection to the DNS server, from the connection to the target server.
		resolveCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), DNSResolveTimeout, errors.New("DNS resolve timeout"))
		defer cancel()

		// We are using custom DNS resolving to detect if the target host - data app, is running.
		// The DNS query is not recursive and not cached, see "dns" package for details.
		ip, err := dnsClient.Resolve(resolveCtx, host) //nolint:contextcheck

		// Trace DNSDone
		if trace != nil && trace.DNSDone != nil {
			trace.DNSDone(httptrace.DNSDoneInfo{
				Addrs: []net.IPAddr{{IP: net.ParseIP(ip)}},
				Err:   err,
			})
		}

		// Handle DNS error
		dnsSpan.End(&err)
		if err != nil {
			return nil, err
		}

		// Dial
		ctx, dialSpan := tel.Tracer().Start(ctx, "keboola.go.apps-proxy.transport.dial")
		dialSpan.SetAttributes(
			attribute.String("transport.dial.network", network),
			attribute.String("transport.dial.ip", ip),
			attribute.String("transport.dial.port", port),
		)
		conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip, port))
		dialSpan.End(&err)
		return conn, err
	}

	httpTransport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialContext,
		ForceAttemptHTTP2:     true, // HTTP2 is preferred.
		DisableKeepAlives:     false,
		TLSHandshakeTimeout:   TLSHandshakeTimeout,
		ResponseHeaderTimeout: ResponseHeaderTimeout,
		MaxIdleConns:          MaxIdleConnections,
		MaxConnsPerHost:       MaxConnectionsPerHost,
		MaxIdleConnsPerHost:   MaxConnectionsPerHost,
	}

	// Configure HTTP2
	http2Transport, err := http2.ConfigureTransports(httpTransport)
	if err != nil {
		return nil, err
	}

	http2Transport.ReadIdleTimeout = HTTP2ReadIdleTimeout
	http2Transport.PingTimeout = HTTP2PingTimeout
	http2Transport.WriteByteTimeout = HTTP2WriteByteTimeout

	// Wrap the transport with telemetry
	return otelhttp.NewTransport(
		httpTransport,
		otelhttp.WithTracerProvider(d.Telemetry().TracerProvider()),
		otelhttp.WithMeterProvider(d.Telemetry().MeterProvider()),
	), nil
}
