package transport

import (
	"github.com/hashicorp/yamux"
)

type ClientStream struct {
	stream *yamux.Stream
	client *Client
}

func newClientStream(stream *yamux.Stream, c *Client) *ClientStream {
	c.registerStream(stream)
	return &ClientStream{stream: stream, client: c}
}

func (s *ClientStream) Close() error {
	s.client.unregisterStream(s.stream)
	return s.stream.Close()
}

func (s *ClientStream) Read(b []byte) (n int, err error) {
	return s.stream.Read(b)
}

func (s *ClientStream) Write(b []byte) (n int, err error) {
	return s.stream.Write(b)
}
