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

const DNSReadTimeout = 2 * time.Second

const DNSWriteTimeout = 2 * time.Second

type Client struct {
	client    *dns.Client
	dnsServer string
}

func NewClient(dialer *net.Dialer) (*Client, error) {
	// DNS client
	client := &dns.Client{
		Net:          "udp",
		Dialer:       dialer,
		DialTimeout:  DialTimeout,
		ReadTimeout:  DNSReadTimeout,
		WriteTimeout: DNSWriteTimeout,
	}

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

	c := &Client{
		client:    client,
		dnsServer: dnsServer,
	}

	return c, nil
}

func (c *Client) Resolve(ctx context.Context, host string) (string, error) {
	msg := &dns.Msg{}
	msg.SetQuestion(dns.Fqdn(host), dns.TypeA)
	msg.Authoritative = true
	msg.RecursionAvailable = false
	msg.RecursionDesired = false

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
