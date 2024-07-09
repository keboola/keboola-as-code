package transport

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/xtaci/kcp-go/v5"
	"net"
)

type kcpTransport struct {
	config network.Config
}

func newKcpTransport(config network.Config) Transport {
	return &kcpTransport{config: config}
}

func (t *kcpTransport) Listen() (net.Listener, error) {
	listener, err := kcp.ListenWithOptions(t.config.Listen, nil, 0, 0)
	if err != nil {
		return nil, errors.PrefixError(err, "cannot create listener")
	}

	// Setup buffer sizes (reversed as on the client side)
	if err := listener.SetReadBuffer(int(t.config.InputBuffer.Bytes())); err != nil {
		return nil, errors.PrefixError(err, "cannot set read buffer size")
	}
	if err := listener.SetWriteBuffer(int(t.config.ResponseBuffer.Bytes())); err != nil {
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
		t.setupConnection(conn)
	}

	return conn, nil
}

func (t *kcpTransport) Dial(addr string) (net.Conn, error) {
	conn, err := kcp.DialWithOptions(addr, nil, 0, 0)
	if err != nil {
		return nil, err
	}

	// Setup buffer sizes (reversed as on the server side)
	if err := conn.SetReadBuffer(int(t.config.ResponseBuffer.Bytes())); err != nil {
		return nil, errors.PrefixError(err, "cannot set read buffer size")
	}
	if err := conn.SetWriteBuffer(int(t.config.InputBuffer.Bytes())); err != nil {
		return nil, errors.PrefixError(err, "cannot set write buffer size")
	}

	t.setupConnection(conn)

	return conn, nil
}

func (*kcpTransport) setupConnection(conn *kcp.UDPSession) {
	conn.SetStreamMode(true)
	conn.SetNoDelay(1, 5, 2, 1)
}
