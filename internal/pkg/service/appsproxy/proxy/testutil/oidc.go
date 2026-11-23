package testutil

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/oauth2-proxy/mockoidc"
	"github.com/stretchr/testify/require"
	"github.com/umisama/go-regexpcache"
)

// idpHostPattern matches the public-looking host name of a test identity provider.
// The name carries the local port, so IDPTransport knows where to connect.
const idpHostPattern = `^idp-(\d+)\.example\.com(:443)?$`

// StartOIDCProviderServer starts a mock OIDC provider at https://idp-<port>.example.com.
// Clients reach it through IDPTransport or IDPDialAddr.
func StartOIDCProviderServer(t *testing.T) *mockoidc.MockOIDC {
	t.Helper()

	m, err := mockoidc.NewServer(nil)
	require.NoError(t, err)

	ln, _ := listenIDP(t)
	tlsConfig := IDPTLSConfig()
	require.NoError(t, m.Start(tls.NewListener(ln, tlsConfig), tlsConfig))
	return m
}

// StartIDPServer starts an HTTPS server at https://idp-<port>.example.com and returns its URL.
func StartIDPServer(t *testing.T, handler http.Handler) string {
	t.Helper()

	ln, host := listenIDP(t)
	srv := &http.Server{Handler: handler, TLSConfig: IDPTLSConfig()} //nolint:gosec // test server
	go func() { _ = srv.Serve(tls.NewListener(ln, srv.TLSConfig)) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "https://" + host
}

// IDPTLSConfig returns the server TLS config of a test identity provider.
// It uses the net/http/httptest certificate, which is valid for *.example.com.
func IDPTLSConfig() *tls.Config {
	srv := httptest.NewUnstartedServer(http.NotFoundHandler())
	srv.StartTLS()
	defer srv.Close()
	return &tls.Config{Certificates: srv.TLS.Certificates, MinVersion: tls.VersionTLS12}
}

// IDPTransport returns a transport that connects test identity provider names to the local server
// and trusts their certificate.
func IDPTransport() *http.Transport {
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
		return dialer.DialContext(ctx, network, IDPDialAddr(addr))
	}
	return transport
}

// IDPDialAddr maps the address of a test identity provider to its local address.
// Any other address is returned unchanged.
func IDPDialAddr(addr string) string {
	m := regexpcache.MustCompile(idpHostPattern).FindStringSubmatch(addr)
	if m == nil {
		return addr
	}
	return net.JoinHostPort("127.0.0.1", m[1])
}

func listenIDP(t *testing.T) (net.Listener, string) {
	t.Helper()

	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	host := fmt.Sprintf("idp-%d.example.com", ln.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert
	return idpListener{Listener: ln, host: host}, host
}

// idpListener reports the test identity provider name as its address, so mockoidc builds its URLs from it.
type idpListener struct {
	net.Listener
	host string
}

func (l idpListener) Addr() net.Addr {
	return idpAddr(l.host)
}

type idpAddr string

func (a idpAddr) Network() string { return "tcp" }

func (a idpAddr) String() string { return string(a) }
