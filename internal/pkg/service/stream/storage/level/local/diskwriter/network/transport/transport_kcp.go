package transport

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/xtaci/kcp-go/v5"
	"net"
)

type Transport interface {
	// Listen for connections by the server.
	Listen(config network.Config) (net.Listener, error)
	// Accept new connection by the server.
	Accept(listener net.Listener) (net.Conn, error)
	// Dial a new connection from the client.
	Dial(addr string, config network.Config) (net.Conn, error)
}

type kcpTransport struct{}

func (*kcpTransport) Listen(config network.Config) (net.Listener, error) {
	listener, err := kcp.ListenWithOptions(config.Listen, nil, 0, 0)
	if err != nil {
		return nil, errors.PrefixError(err, "cannot create listener")
	}

	// Setup buffer sizes (reversed as on the client side)
	if err := listener.SetReadBuffer(int(config.InputBuffer.Bytes())); err != nil {
		return nil, errors.PrefixError(err, "cannot set read buffer size")
	}
	if err := listener.SetWriteBuffer(int(config.ResponseBuffer.Bytes())); err != nil {
		return nil, errors.PrefixError(err, "cannot set write buffer size")
	}

	return listener, nil
}

func (t *kcpTransport) Accept(listener net.Listener) (net.Conn, error) {
	conn, err := listener.Accept()
	if err != nil {
		return nil, err
	}

	if conn, ok := conn.(*kcp.UDPSession); ok {
		t.setupKCPConnection(conn)
	}

	return conn, nil
}

func (t *kcpTransport) Dial(addr string, config network.Config) (net.Conn, error) {
	conn, err := kcp.DialWithOptions(addr, nil, 0, 0)
	if err != nil {
		return nil, err
	}

	// Setup buffer sizes (reversed as on the server side)
	if err := conn.SetReadBuffer(int(config.ResponseBuffer.Bytes())); err != nil {
		return nil, errors.PrefixError(err, "cannot set read buffer size")
	}
	if err := conn.SetWriteBuffer(int(config.InputBuffer.Bytes())); err != nil {
		return nil, errors.PrefixError(err, "cannot set write buffer size")
	}

	t.setupKCPConnection(conn)

	return conn, nil
}

func (*kcpTransport) setupKCPConnection(conn *kcp.UDPSession) {
	conn.SetStreamMode(true)
	conn.SetNoDelay(1, 5, 2, 1)
}
