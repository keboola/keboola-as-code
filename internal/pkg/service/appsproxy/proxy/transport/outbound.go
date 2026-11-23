package transport

import (
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// NewPublicOnlyHTTPClient creates a client for requests to authentication providers.
func NewPublicOnlyHTTPClient(userAgent string) *http.Client {
	return &http.Client{Transport: &userAgentTransport{next: NewPublicOnlyTransport(), userAgent: userAgent}}
}

// NewPublicOnlyTransport creates a transport that connects only to public unicast addresses.
// The address is checked after DNS resolution, so it also covers redirects and DNS rebinding.
func NewPublicOnlyTransport() *http.Transport {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   checkPublicAddress,
	}

	httpTransport := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert
	// An environment proxy would make the dialer check the proxy address instead of the target.
	httpTransport.Proxy = nil
	httpTransport.DialContext = dialer.DialContext

	return httpTransport
}

func checkPublicAddress(_, address string, _ syscall.RawConn) error {
	addrPort, err := netip.ParseAddrPort(address)
	if err != nil {
		return errors.Errorf(`connection to "%s" is not allowed: %w`, address, err)
	}

	addr := addrPort.Addr().Unmap()
	if addr.IsLoopback() ||
		addr.IsLinkLocalUnicast() ||
		addr.IsLinkLocalMulticast() ||
		addr.IsInterfaceLocalMulticast() ||
		addr.IsMulticast() ||
		addr.IsPrivate() ||
		addr.IsUnspecified() ||
		inBlockedPrefix(addr) {
		return errors.Errorf(`connection to "%s" is not allowed, the address is not public`, address)
	}

	return nil
}

func inBlockedPrefix(addr netip.Addr) bool {
	for _, prefix := range blockedPrefixes() {
		if prefix.Contains(addr) {
			return true
		}
	}
	return false
}

// blockedPrefixes are the non-public ranges that the netip.Addr predicates do not cover.
func blockedPrefixes() []netip.Prefix {
	return []netip.Prefix{
		netip.MustParsePrefix("100.64.0.0/10"),    // carrier-grade NAT
		netip.MustParsePrefix("168.63.129.16/32"), // Azure WireServer
		netip.MustParsePrefix("192.0.0.0/24"),     // IETF protocol assignments, incl. Oracle Cloud metadata
		netip.MustParsePrefix("192.88.99.0/24"),   // 6to4 relay anycast
		netip.MustParsePrefix("::/96"),            // IPv4-compatible
		netip.MustParsePrefix("64:ff9b::/96"),     // NAT64
		netip.MustParsePrefix("64:ff9b:1::/48"),   // local-use NAT64
		netip.MustParsePrefix("100::/64"),         // discard-only
		netip.MustParsePrefix("2001::/32"),        // Teredo
		netip.MustParsePrefix("2002::/16"),        // 6to4
	}
}

type userAgentTransport struct {
	next      http.RoundTripper
	userAgent string
}

func (t *userAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("User-Agent") != "" {
		return t.next.RoundTrip(req)
	}
	req = req.Clone(req.Context())
	req.Header.Set("User-Agent", t.userAgent)
	return t.next.RoundTrip(req)
}
