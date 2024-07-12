package transport

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/yamux"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type serverDependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
}

type Server struct {
	logger     log.Logger
	config     network.Config
	transport  Transport
	handler    StreamHandler
	listenAddr net.Addr

	closed    chan struct{}
	listenWg  sync.WaitGroup
	streamsWg sync.WaitGroup

	lock     sync.Mutex
	sessions map[string]*yamux.Session
	streams  map[string]*yamux.Stream
}

type StreamHandler func(ctx context.Context, stream *yamux.Stream)

// Listen function is called by the Writer node, to listen for slices data.
func Listen(d serverDependencies, config network.Config, nodeID string, handler StreamHandler) (*Server, error) {
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
		logger:    d.Logger().WithComponent("storage.node.writer.network.server"),
		config:    config,
		transport: transport,
		handler:   handler,
		closed:    make(chan struct{}),
		sessions:  make(map[string]*yamux.Session),
		streams:   make(map[string]*yamux.Stream),
	}

	// Create listener
	listener, err := s.listen(ctx)
	if err != nil {
		return nil, err
	}

	// Graceful shutdown
	d.Process().OnShutdown(func(_ context.Context) {
		s.logger.Info(ctx, "closing disk writer server")

		// Prevent new connections and streams to be opened
		close(s.closed)

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
		if err := listener.Close(); err != nil {
			err = errors.PrefixError(err, "cannot close listener")
			s.logger.Error(ctx, err.Error())
		}

		// Wait for remaining goroutines
		s.listenWg.Wait()

		s.logger.Info(ctx, "closed disk writer server")
	})

	// Accept connections in background
	s.listenWg.Add(1)
	go func() {
		defer s.listenWg.Done()
		s.acceptConnectionsLoop(ctx, listener)
	}()

	return s, nil
}

func (s *Server) ListenAddr() net.Addr {
	return s.listenAddr
}

// listen creates a TCP like listener using the kcp-go library.
//   - No encryption - access limited by the Kubernetes network policy
//   - No FEC - Forward Error Correction - a reliable network is assumed
func (s *Server) listen(ctx context.Context) (net.Listener, error) {
	listener, err := s.transport.Listen()
	if err != nil {
		return nil, err
	}

	s.listenAddr = listener.Addr()
	s.logger.Infof(ctx, `disk writer listening on %q`, s.listenAddr.String())
	return listener, nil
}

func (s *Server) acceptConnectionsLoop(ctx context.Context, listener net.Listener) {
	b := newServerBackoff()
	for {
		if s.isClosed() {
			return
		}

		if err := s.acceptConnection(ctx, listener); err != nil && !s.isClosed() {
			delay := b.NextBackOff()
			err = errors.Errorf(`cannot accept connection: %w, waiting %s`, err, delay)
			s.logger.Error(ctx, err.Error())
			<-time.After(delay)
			continue
		}

		b.Reset()
	}
}

func (s *Server) acceptConnection(ctx context.Context, listener net.Listener) error {
	// Accept connection
	conn, err := s.transport.Accept(listener)
	if err != nil {
		return err
	}

	// Create multiplexer
	sess, err := yamux.Server(conn, multiplexerConfig(s.logger, s.config))
	if err != nil {
		_ = conn.Close()
		return errors.PrefixError(err, "cannot create server multiplexer")
	}

	s.logger.Infof(ctx, "accepted connection from %q", conn.RemoteAddr().String())

	// Span goroutine for each connection
	s.listenWg.Add(1)
	s.registerSession(sess)
	go func() {
		defer s.listenWg.Done()
		s.acceptStreamsLoop(ctx, sess)
		s.unregisterSession(sess)
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

		if err := s.acceptStream(ctx, sess); err != nil && !s.isClosed() && !sess.IsClosed() {
			delay := b.NextBackOff()
			err = errors.Errorf(`cannot accept stream: %w, waiting %s`, err, delay)
			s.logger.Error(ctx, err.Error())
			<-time.After(delay)
			continue
		}

		b.Reset()
	}
}

func (s *Server) acceptStream(ctx context.Context, sess *yamux.Session) error {
	// Accept stream
	stream, err := sess.AcceptStream()
	if err != nil {
		return err
	}

	// Spawn a goroutine for each stream
	s.listenWg.Add(1)
	s.registerStream(stream)
	go func() {
		defer s.listenWg.Done()
		s.handler(ctx, stream)
		if err := stream.Close(); err != nil {
			err = errors.PrefixError(err, "cannot close stream")
			s.logger.Error(ctx, err.Error())
		}
		s.unregisterStream(stream)
	}()

	return nil
}

func (s *Server) registerSession(sess *yamux.Session) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.sessions[sessionKey(sess)] = sess
}

func (s *Server) unregisterSession(sess *yamux.Session) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.sessions, sessionKey(sess))
}

func (s *Server) registerStream(stream *yamux.Stream) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.streamsWg.Add(1)
	s.streams[streamKey(stream)] = stream
}

func (s *Server) unregisterStream(stream *yamux.Stream) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.streams, streamKey(stream))
	s.streamsWg.Done()
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
