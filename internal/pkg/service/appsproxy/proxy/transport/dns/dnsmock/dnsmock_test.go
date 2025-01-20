// Based on https://github.com/kuritka/go-fake-dns
// License: MIT

package dnsmock_test

import (
	"net"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/transport/dns/dnsmock"
)

func TestMultipleTXTRecords(t *testing.T) {
	t.Parallel()

	dnsMock := dnsmock.New(0)
	dnsMock.AddTXTRecord("heartbeat-us.cloud.example.com.", "1")
	dnsMock.AddTXTRecord("heartbeat-uk.cloud.example.com.", "2")
	dnsMock.AddTXTRecord("heartbeat-eu.cloud.example.com.", "0", "6", "8")
	err := dnsMock.Start()
	require.NoError(t, err)

	defer dnsMock.Shutdown()

	g := new(dns.Msg)
	g.SetQuestion("ip.blah.cloud.example.com.", dns.TypeTXT)
	a, err := dns.Exchange(g, dnsMock.Addr())
	require.NoError(t, err)
	require.Empty(t, a.Answer)

	g = new(dns.Msg)
	g.SetQuestion("heartbeat-uk.cloud.example.com.", dns.TypeTXT)
	a, err = dns.Exchange(g, dnsMock.Addr())
	require.NoError(t, err)
	require.Len(t, a.Answer, 1)
	require.Len(t, a.Answer[0].(*dns.TXT).Txt, 1)
	require.Equal(t, "2", a.Answer[0].(*dns.TXT).Txt[0])

	g = new(dns.Msg)
	g.SetQuestion("heartbeat-eu.cloud.example.com.", dns.TypeTXT)
	a, err = dns.Exchange(g, dnsMock.Addr())
	require.NoError(t, err)
	require.Len(t, a.Answer, 1)
	require.Len(t, a.Answer[0].(*dns.TXT).Txt, 3)
	require.Equal(t, "0", a.Answer[0].(*dns.TXT).Txt[0])
	require.Equal(t, "6", a.Answer[0].(*dns.TXT).Txt[1])
	require.Equal(t, "8", a.Answer[0].(*dns.TXT).Txt[2])
}

func TestSimple(t *testing.T) {
	t.Parallel()

	dnsMock := dnsmock.New(0)
	dnsMock.AddNSRecord("blah.cloud.example.com.", "gslb-ns-us-cloud.example.com.")
	dnsMock.AddNSRecord("blah.cloud.example.com.", "gslb-ns-uk-cloud.example.com.")
	dnsMock.AddNSRecord("blah.cloud.example.com.", "gslb-ns-eu-cloud.example.com.")
	dnsMock.AddTXTRecord("First", "Second", "Banana")
	dnsMock.AddTXTRecord("White", "Red", "Purple")
	dnsMock.AddARecord("ip.blah.cloud.example.com.", net.IPv4(10, 0, 1, 5))
	err := dnsMock.Start()
	require.NoError(t, err)

	defer dnsMock.Shutdown()

	g := new(dns.Msg)
	g.SetQuestion("ip.blah.cloud.example.com.", dns.TypeA)
	a, err := dns.Exchange(g, dnsMock.Addr())
	require.NoError(t, err)
	require.NotEmpty(t, a.Answer)

	dnsMock.RemoveARecords("ip.blah.cloud.example.com.")
	a, err = dns.Exchange(g, dnsMock.Addr())
	require.NoError(t, err)
	require.Empty(t, a.Answer)
}

func TestWrongARecord(t *testing.T) {
	t.Parallel()

	dnsMock := dnsmock.New(0)
	require.Error(t, dnsMock.AddARecord("ipv4.net.", net.IPv6loopback))
	require.Error(t, dnsMock.AddAAAARecord("ipv6.net.", net.IP{0, 0}))
	err := dnsMock.Start()
	require.NoError(t, err)

	defer dnsMock.Shutdown()

	g := new(dns.Msg)
	g.SetQuestion("ipv4.net.", dns.TypeA)
	a, err := dns.Exchange(g, dnsMock.Addr())
	require.NoError(t, err)
	require.Empty(t, a.Answer)

	g.SetQuestion("ipv6.net.", dns.TypeAAAA)
	a, err = dns.Exchange(g, dnsMock.Addr())
	require.NoError(t, err)
	require.Empty(t, a.Answer)

}
