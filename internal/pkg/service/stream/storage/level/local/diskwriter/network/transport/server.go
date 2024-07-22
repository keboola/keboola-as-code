package transport

import (
	"context"
	"io"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/yamux"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Server struct {
	logger    log.Logger
	config    network.Config
	transport Transport
	listener  net.Listener

	accept    chan *ServerStream
	listenWg  sync.WaitGroup
	streamsWg sync.WaitGroup

	closeLock sync.Mutex
	closed    chan struct{}

	lock     sync.Mutex
	sessions map[string]*yamux.Session
	streams  map[string]*ServerStream
}

type StreamHandler func(ctx context.Context, stream *yamux.Stream)

// Listen function is called by the Writer node, to listen for slices data.
func Listen(logger log.Logger, config network.Config, nodeID string) (*Server, error) {
	ctx := ctxattr.ContextWith(
		context.Background(),
		attribute.String("nodeId", nodeID),
		attribute.String("listenAddress", config.Listen),
	)

	transport, err := newTransport(config)
	if err != nil {
		return nil, err
	}

	s := &Server{
		logger:    logger,
		config:    config,
		transport: transport,
		accept:    make(chan *ServerStream),
		closed:    make(chan struct{}),
		sessions:  make(map[string]*yamux.Session),
		streams:   make(map[string]*ServerStream),
	}

	// Create listener
	if err := s.listen(ctx); err != nil {
		return nil, err
	}

	// Accept connections in background
	s.listenWg.Add(1)
	go func() {
		defer s.listenWg.Done()
		s.acceptConnectionsLoop(ctx)
	}()

	return s, nil
}

func (s *Server) Accept() (net.Conn, error) {
	for {
		select {
		case stream := <-s.accept:
			return stream, nil
		case <-s.closed:
			return nil, io.ErrClosedPipe
		}
	}
}

func (s *Server) Addr() net.Addr {
	return s.listener.Addr()
}

func (s *Server) Close() error {
	s.closeLock.Lock()
	defer s.closeLock.Unlock()

	if s.isClosed() {
		return nil
	}

	// Prevent new connections and streams to be opened.
	// Unblock the Accept method, see net.Listener interface for details.
	close(s.closed)

	ctx := context.Background()

	s.logger.Info(ctx, "closing disk writer server")

	// Waiting for streams
	{
		s.lock.Lock()
		streamsCount := len(s.streams)
		s.lock.Unlock()

		s.logger.Infof(ctx, "waiting %s for %d streams", s.config.ShutdownTimeout, streamsCount)

		done := make(chan struct{})
		go func() {
			defer close(done)
			s.streamsWg.Wait()
		}()

		select {
		case <-done:
			s.logger.Info(ctx, `waiting for streams done`)
		case <-time.After(s.config.ShutdownTimeout):
			s.logger.Infof(ctx, `waiting for streams timeout after %s`, s.config.ShutdownTimeout)
		}
	}

	// Close streams
	{
		s.lock.Lock()
		streams := maps.Values(s.streams)
		s.lock.Unlock()
		s.logger.Infof(ctx, "closing %d streams", len(streams))
		wg := &sync.WaitGroup{}
		for _, stream := range streams {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := stream.Close(); err != nil {
					err = errors.PrefixError(err, "cannot close server stream")
					s.logger.Error(ctx, err.Error())
				}
			}()
		}
		wg.Wait()
	}

	// Close sessions
	{
		s.lock.Lock()
		sessions := maps.Values(s.sessions)
		s.lock.Unlock()
		s.logger.Infof(ctx, "closing %d sessions", len(sessions))
		wg := &sync.WaitGroup{}
		for _, sess := range sessions {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := sess.Close(); err != nil {
					err = errors.PrefixError(err, "cannot close server session")
					s.logger.Error(ctx, err.Error())
				}
			}()
		}
		wg.Wait()
	}

	// Close listener
	if err := s.listener.Close(); err != nil {
		err = errors.PrefixError(err, "cannot close listener")
		s.logger.Error(ctx, err.Error())
	}

	// Wait for remaining goroutines
	s.logger.Info(ctx, "waiting for goroutines")
	s.listenWg.Wait()

	s.logger.Info(ctx, "closed disk writer server")
	return nil
}

func (s *Server) Port() string {
	_, port, _ := net.SplitHostPort(s.Addr().String())
	return port
}

func (s *Server) listen(ctx context.Context) error {
	listener, err := s.transport.Listen()
	if err != nil {
		return err
	}

	s.listener = listener
	s.logger.Infof(ctx, `disk writer listening on %q`, s.listener.Addr().String())
	return nil
}

func (s *Server) acceptConnectionsLoop(ctx context.Context) {
	b := newServerBackoff()
	for {
		if s.isClosed() {
			return
		}

		if err := s.acceptConnection(ctx); err != nil && !s.isClosed() {
			delay := b.NextBackOff()
			err = errors.Errorf(`cannot accept connection: %w, waiting %s`, err, delay)
			s.logger.Error(ctx, err.Error())
			<-time.After(delay)
			continue
		}

		b.Reset()
	}
}

func (s *Server) acceptConnection(ctx context.Context) error {
	// Accept connection
	conn, err := s.transport.Accept(s.listener)
	if err != nil {
		return err
	}

	if s.isClosed() {
		_ = conn.Close()
		return nil
	}

	// Create multiplexer
	sess, err := yamux.Server(conn, multiplexerConfig(s.logger, s.config))
	if err != nil {
		_ = conn.Close()
		return errors.PrefixError(err, "cannot create server multiplexer")
	}

	s.logger.Infof(ctx, "accepted connection from %q to %q", conn.RemoteAddr().String(), conn.LocalAddr().String())

	// Span goroutine for each connection
	s.listenWg.Add(1)
	s.registerSession(ctx, sess)
	go func() {
		defer s.listenWg.Done()
		s.acceptStreamsLoop(ctx, sess)
		s.unregisterSession(ctx, sess)
		s.logger.Infof(ctx, "closed connection from %q", conn.RemoteAddr().String())
	}()

	return nil
}

func (s *Server) acceptStreamsLoop(ctx context.Context, sess *yamux.Session) {
	b := newServerBackoff()
	for {
		if s.isClosed() || sess.IsClosed() {
			return
		}

		if err := s.acceptStream(sess); err != nil && !s.isClosed() && !sess.IsClosed() {
			delay := b.NextBackOff()
			err = errors.Errorf(`cannot accept stream: %w, waiting %s`, err, delay)
			s.logger.Error(ctx, err.Error())
			<-time.After(delay)
			continue
		}

		b.Reset()
	}
}

func (s *Server) acceptStream(sess *yamux.Session) error {
	// Accept stream
	ys, err := sess.AcceptStream()
	if err != nil {
		return err
	}

	if s.isClosed() || sess.IsClosed() {
		_ = ys.Close()
		return nil
	}

	// Spawn a goroutine for each stream
	stream := newServerStream(ys, s)
	select {
	case <-s.closed:
		_ = stream.Close()
	case s.accept <- stream:
		// ok
	}

	return nil
}

func (s *Server) registerSession(ctx context.Context, sess *yamux.Session) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.sessions[sessionKey(sess)] = sess
	s.logger.Debugf(ctx, "registered session to %q, total sessions count %d", sess.RemoteAddr(), len(s.sessions))
}

func (s *Server) unregisterSession(ctx context.Context, sess *yamux.Session) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.sessions, sessionKey(sess))
	s.logger.Debugf(ctx, "unregistered session to %q, total sessions count %d", sess.RemoteAddr(), len(s.sessions))
}

func (s *Server) registerStream(ctx context.Context, stream *ServerStream) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.streamsWg.Add(1)
	s.streams[streamKey(stream)] = stream
	s.logger.Debugf(ctx, `registered stream "%d" to %q, total streams count %d`, stream.StreamID(), stream.RemoteAddr(), len(s.streams))
}

func (s *Server) unregisterStream(ctx context.Context, stream *ServerStream) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.streams, streamKey(stream))
	s.streamsWg.Done()
	s.logger.Debugf(ctx, `unregistered stream "%d" to %q, total streams count %d`, stream.StreamID(), stream.RemoteAddr(), len(s.streams))
}

func (s *Server) isClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}

func newServerBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.2
	b.Multiplier = 2
	b.InitialInterval = 5 * time.Millisecond
	b.MaxInterval = 5 * time.Second
	b.MaxElapsedTime = 0 // don't stop
	b.Reset()
	return b
}
