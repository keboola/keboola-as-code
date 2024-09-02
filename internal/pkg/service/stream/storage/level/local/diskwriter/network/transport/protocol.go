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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport/tcp"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Protocol interface {
	Type() network.TransportProtocol
	// Listen for connections by the server.
	Listen() (net.Listener, error)
	// Accept new connection by the server.
	Accept(listener net.Listener) (net.Conn, error)
	// Dial a new connection from the client.
	Dial(addr string) (net.Conn, error)
}

func NewProtocol(config network.Config) (Protocol, error) {
	switch config.Transport {
	// KCP is creating time scheduler instance even it is only imported, we don't use it now.
	// case network.TransportProtocolKCP:
	//	return kcp.New(config), nil
	case network.TransportProtocolTCP:
		return tcp.New(config), nil
	default:
		return nil, errors.Errorf(`unexpected transport protocol %q`, config.Transport)
	}
}
