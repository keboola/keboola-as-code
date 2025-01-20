package testutil

import (
	"errors"
	"net"
	"net/url"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/transport/dns/dnsmock"
)

func StartDNSServer(t *testing.T, port int) *dnsmock.Server {
	t.Helper()

	server := dnsmock.New(port)
	err := server.Start()
	require.NoError(t, err)

	return server
}

func AddAppDNSRecord(t *testing.T, appServer *AppServer, dnsServer *dnsmock.Server) (appURL *url.URL) {
	t.Helper()

	tsURL, err := url.Parse(appServer.URL)
	require.NoError(t, err)

	ip, _, err := net.SplitHostPort(tsURL.Host)
	require.NoError(t, err)

	appHost := "app.local"
	dnsServer.AddARecord(dns.Fqdn(appHost), net.ParseIP(ip))

	return &url.URL{
		Scheme: tsURL.Scheme,
		Host:   net.JoinHostPort(appHost, tsURL.Port()),
	}
}
