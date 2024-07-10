// Package transport provides transport layer for communication between source and disk writer nodes.
//
// Communication is based on KCP Reliable UDP protocol and yamux streams multiplexer.
// TCP is an old protocol with several issues regarding latency.
// It is not possible to make major changes to TCP, as it is a widely used standard.
// Therefore, R-UDP protocols are used to address issues.
package transport

import (
	"net"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Transport interface {
	// Listen for connections by the server.
	Listen() (net.Listener, error)
	// Accept new connection by the server.
	Accept(listener net.Listener) (net.Conn, error)
	// Dial a new connection from the client.
	Dial(addr string) (net.Conn, error)
}

func newTransport(config network.Config) (Transport, error) {
	switch config.Transport {
	case network.TransportProtocolKCP:
		return newKCPTransport(config), nil
	case network.TransportProtocolTCP:
		return newTCPTransport(config), nil
	default:
		return nil, errors.Errorf(`unexpected transport protocol %q`, config.Transport)
	}
}
