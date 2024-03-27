package http

import (
	"net"
	"time"
)

// DialTimeout specifies the default maximum connection initialization time.
const DialTimeout = 2 * time.Second

// KeepAlive specifies the default interval between keep-alive probes.
const KeepAlive = 15 * time.Second

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
