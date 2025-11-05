package transport

import (
	"net"
	"time"
)

// DialTimeout specifies the default maximum connection initialization time.
const DialTimeout = 100 * time.Millisecond

// KeepAlive specifies the default interval between keep-alive probes.
const KeepAlive = 15 * time.Second

// newDialer creates dialer for DNS resolving and the HTTP transport.
func newDialer() *net.Dialer {
	return &net.Dialer{
		Timeout:   DialTimeout,
		KeepAlive: KeepAlive,
		Resolver: &net.Resolver{
			PreferGo:     true,
			StrictErrors: true,
		},
	}
}
