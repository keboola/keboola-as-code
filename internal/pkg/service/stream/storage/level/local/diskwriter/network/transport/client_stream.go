package transport

import (
	"sync"

	"github.com/hashicorp/yamux"
)

// ClientStream implements net.Conn.
type ClientStream struct {
	*yamux.Stream
	conn *ClientConnection

	closeLock sync.Mutex
	closed    bool
}

func newClientStream(conn *ClientConnection, stream *yamux.Stream) *ClientStream {
	s := &ClientStream{conn: conn, Stream: stream}
	conn.registerStream(s)
	return s
}

func (s *ClientStream) IsConnected() bool {
	return s.conn.IsConnected()
}

func (s *ClientStream) Close() error {
	s.closeLock.Lock()
	defer s.closeLock.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	err := s.Stream.Close()
	s.conn.unregisterStream(s)
	return err
}
