package tcp

import (
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

func (t *Protocol) Listen() (net.Listener, error) {
	return net.Listen("tcp", t.config.Listen)
}

func (t *Protocol) Accept(listener net.Listener) (net.Conn, error) {
	return listener.Accept()
}

func (t *Protocol) Dial(addr string) (net.Conn, error) {
	return net.Dial("tcp", addr)
}
