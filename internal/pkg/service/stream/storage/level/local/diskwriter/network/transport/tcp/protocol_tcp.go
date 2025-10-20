package tcp

import (
	"context"
	"net"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
)

type Protocol struct {
	config network.Config
}

func New(config network.Config) *Protocol {
	return &Protocol{config: config}
}

func (t *Protocol) Type() network.TransportProtocol {
	return network.TransportProtocolTCP
}

func (t *Protocol) Listen(ctx context.Context) (net.Listener, error) {
	var lc net.ListenConfig
	return lc.Listen(ctx, "tcp", t.config.Listen)
}

func (t *Protocol) Accept(listener net.Listener) (net.Conn, error) {
	return listener.Accept()
}

func (t *Protocol) Dial(ctx context.Context, addr string) (net.Conn, error) {
	dialer := net.Dialer{}
	return dialer.DialContext(ctx, "tcp", addr)
}
