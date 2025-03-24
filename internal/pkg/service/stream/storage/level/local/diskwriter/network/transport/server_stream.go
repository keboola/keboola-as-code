package transport

import (
	"context"

	"github.com/hashicorp/yamux"
	"github.com/sasha-s/go-deadlock"
)

// ServerStream implements net.Conn.
type ServerStream struct {
	*yamux.Stream
	server *Server

	closeLock deadlock.Mutex
	closed    bool
}

func newServerStream(ys *yamux.Stream, server *Server) *ServerStream {
	s := &ServerStream{Stream: ys, server: server}
	server.registerStream(context.Background(), s)
	return s
}

func (s *ServerStream) Close() error {
	s.closeLock.Lock()
	defer s.closeLock.Unlock()

	if s.closed {
		return nil
	}

	err := s.Stream.Close()
	s.server.unregisterStream(context.Background(), s)
	s.closed = true
	return err
}
