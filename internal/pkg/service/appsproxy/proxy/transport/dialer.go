package transport

import (
	"net"
	"time"
)

// DialTimeout specifies the default maximum connection initialization time.
const DialTimeout = 2 * time.Second

// KeepAlive specifies the default interval between keep-alive probes.
const KeepAlive = 15 * time.Second

// newDialer creates dialer for DNS resolving and the HTTP transport.
func newDialer() *net.Dialer {
	return &net.Dialer{
		Timeout:   DialTimeout,
		KeepAlive: KeepAlive,
		// TEMPORARY: Use default resolver (no custom config) to match debug behavior
		// Resolver: &net.Resolver{
		// 	PreferGo:     true,
		// 	StrictErrors: true,
		// },
	}
}
