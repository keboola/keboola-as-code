package transport

import (
	"net"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
)

type tcpTransport struct {
	config network.Config
}

func newTCPTransport(config network.Config) Transport {
	return &tcpTransport{config: config}
}

func (t *tcpTransport) Listen() (net.Listener, error) {
	return net.Listen("tcp", t.config.Listen)
}

func (t *tcpTransport) Accept(listener net.Listener) (net.Conn, error) {
	return listener.Accept()
}

func (t *tcpTransport) Dial(addr string) (net.Conn, error) {
	return net.Dial("tcp", addr)
}
