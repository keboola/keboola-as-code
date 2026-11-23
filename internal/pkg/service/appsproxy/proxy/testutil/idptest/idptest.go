// Package idptest serves test identity providers at public-looking host names.
// It is imported by the mocked dependency scope, so it must not import mockoidc.
package idptest

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/umisama/go-regexpcache"
)

// hostPattern matches the host name of a test identity provider.
// The name carries the local port, so Transport knows where to connect.
const hostPattern = `^idp-(\d+)\.example\.com(:443)?$`

// StartServer starts an HTTPS server at https://idp-<port>.example.com and returns its URL.
func StartServer(t *testing.T, handler http.Handler) string {
	t.Helper()

	ln, host := Listen(t)
	srv := &http.Server{Handler: handler, TLSConfig: TLSConfig()} //nolint:gosec // test server
	go func() { _ = srv.Serve(tls.NewListener(ln, srv.TLSConfig)) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "https://" + host
}

// Listen opens a local listener whose address is reported as idp-<port>.example.com.
func Listen(t *testing.T) (net.Listener, string) {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	host := fmt.Sprintf("idp-%d.example.com", ln.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert
	return listener{Listener: ln, host: host}, host
}

// TLSConfig returns the server TLS config of a test identity provider.
// It uses the net/http/httptest certificate, which is valid for *.example.com.
func TLSConfig() *tls.Config {
	srv := httptest.NewUnstartedServer(http.NotFoundHandler())
	srv.StartTLS()
	defer srv.Close()
	return &tls.Config{Certificates: srv.TLS.Certificates, MinVersion: tls.VersionTLS12}
}

// Transport connects test identity provider names to the local server and trusts their certificate.
func Transport() *http.Transport {
	srv := httptest.NewUnstartedServer(http.NotFoundHandler())
	srv.StartTLS()
	defer srv.Close()
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())

	dialer := &net.Dialer{}
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, DialAddr(addr))
	}
	return transport
}

// DialAddr maps the address of a test identity provider to its local address.
// Any other address is returned unchanged.
func DialAddr(addr string) string {
	m := regexpcache.MustCompile(hostPattern).FindStringSubmatch(addr)
	if m == nil {
		return addr
	}
	return net.JoinHostPort("127.0.0.1", m[1])
}

// listener reports the test identity provider name as its address, so servers build their URLs from it.
type listener struct {
	net.Listener
	host string
}

func (l listener) Addr() net.Addr {
	return addr(l.host)
}

type addr string

func (a addr) Network() string { return "tcp" }

func (a addr) String() string { return string(a) }
