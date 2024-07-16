package transport

import (
	"github.com/hashicorp/yamux"
)

type ClientStream struct {
	conn   *ClientConnection
	stream *yamux.Stream
}

func newClientStream(conn *ClientConnection, stream *yamux.Stream) *ClientStream {
	s := &ClientStream{conn: conn, stream: stream}
	conn.registerStream(s)
	return s
}

func (s *ClientStream) StreamID() uint32 {
	return s.stream.StreamID()
}

func (s *ClientStream) Close() error {
	err := s.stream.Close()
	s.conn.unregisterStream(s)
	return err
}

func (s *ClientStream) Read(b []byte) (n int, err error) {
	return s.stream.Read(b)
}

func (s *ClientStream) Write(b []byte) (n int, err error) {
	return s.stream.Write(b)
}
