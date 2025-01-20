package dns

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const DialTimeout = 2 * time.Second

const ReadTimeout = 2 * time.Second

const WriteTimeout = 2 * time.Second

type Client struct {
	client    *dns.Client
	dnsServer string
}

func NewClientFromEtc(dialer *net.Dialer) (*Client, error) {
	// Parse DNS config
	dnsCfgFile := "/etc/resolv.conf"
	dnsCfg, err := dns.ClientConfigFromFile(dnsCfgFile)
	if err != nil {
		return nil, err
	}
	if len(dnsCfg.Servers) == 0 {
		return nil, errors.Errorf(`no DNS server found in "%s"`, dnsCfgFile)
	}

	// Get DNS server
	dnsServer := dnsCfg.Servers[0]
	if !strings.Contains(dnsServer, ":") {
		dnsServer += ":53"
	}

	return NewClient(dialer, dnsServer), nil
}

func NewClient(dialer *net.Dialer, dnsServer string) *Client {
	return &Client{
		client: &dns.Client{
			Net:          "udp",
			Dialer:       dialer,
			DialTimeout:  DialTimeout,
			ReadTimeout:  ReadTimeout,
			WriteTimeout: WriteTimeout,
		},
		dnsServer: dnsServer,
	}
}

func (c *Client) DNSServer() string {
	return c.dnsServer
}

func createDNSMessage(host string, typ uint16) *dns.Msg {
	msg := &dns.Msg{}
	msg.SetQuestion(dns.Fqdn(host), typ)
	msg.Authoritative = true
	// Disable recursion because we want to know if service pod is available in k8s.
	// No need to recursively ask other servers. Also disables caching which we also want.
	msg.RecursionDesired = false
	msg.RecursionAvailable = false
	return msg
}

func (c *Client) Resolve(ctx context.Context, host string) (string, error) {
	msg := createDNSMessage(host, dns.TypeA)

	// Send DNS query
	resp, _, err := c.client.ExchangeContext(ctx, msg, c.dnsServer)
	if err != nil {
		return "", err
	}

	if len(resp.Answer) == 0 {
		return "", &net.DNSError{
			Err:        fmt.Sprintf(`host not found: %s`, host),
			Name:       host,
			Server:     c.dnsServer,
			IsNotFound: true,
		}
	}

	// nolint: gosec // we don't need to use crypto.rand here
	ip := resp.Answer[rand.Intn(len(resp.Answer))].(*dns.A).A.String()
	return ip, nil
}
