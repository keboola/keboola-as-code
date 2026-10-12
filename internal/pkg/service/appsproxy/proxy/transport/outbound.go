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
// It connects only to public unicast addresses.
// The address is checked after DNS resolution, so it also covers redirects and DNS rebinding.
func NewPublicOnlyHTTPClient(userAgent string) *http.Client {
	dialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   checkPublicAddress,
	}

	httpTransport := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert
	// An environment proxy would make the dialer check the proxy address instead of the target.
	httpTransport.Proxy = nil
	httpTransport.DialContext = dialer.DialContext

	return &http.Client{Transport: &userAgentTransport{next: httpTransport, userAgent: userAgent}}
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
		netip.MustParsePrefix("100.64.0.0/10").Contains(addr) {
		return errors.Errorf(`connection to "%s" is not allowed, the address is not public`, address)
	}

	return nil
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
