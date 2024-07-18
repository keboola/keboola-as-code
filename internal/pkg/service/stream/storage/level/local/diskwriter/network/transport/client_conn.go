package transport

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/yamux"
	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ClientConnection struct {
	client *Client

	closed chan struct{}
	wg     sync.WaitGroup

	remoteNodeID string
	remoteAddr   string

	lock      sync.Mutex
	sess      *yamux.Session
	lastError error
	streams   map[uint32]*ClientStream
}

func openClientConnection(ctx context.Context, remoteNodeID, remoteAddr string, client *Client, initDone chan error) (*ClientConnection, error) {
	// Stop, if the client is closed
	if client.isClosed() {
		return nil, yamux.ErrSessionShutdown
	}

	ctx = ctxattr.ContextWith(
		ctx,
		attribute.String("nodeId", client.nodeID),
		attribute.String("remoteNodeID", remoteNodeID),
		attribute.String("remoteAddr", remoteAddr),
	)

	c := &ClientConnection{
		client:       client,
		closed:       make(chan struct{}),
		remoteNodeID: remoteNodeID,
		remoteAddr:   remoteAddr,
		streams:      make(map[uint32]*ClientStream),
	}

	// Dial connection
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.dialLoop(ctx, initDone)
	}()

	client.registerConnection(c)
	return c, nil
}

func (c *ClientConnection) RemoteNodeID() string {
	return c.remoteNodeID
}

func (c *ClientConnection) RemoteAddr() string {
	return c.remoteAddr
}

func (c *ClientConnection) IsConnected() bool {
	sess, err := c.session()
	if err != nil {
		return false
	}

	select {
	case <-sess.CloseChan():
		return false
	default:
		return true
	}
}

func (c *ClientConnection) OpenStream() (*ClientStream, error) {
	// Stop, if the client is closed
	if c.isClosed() || c.client.isClosed() {
		return nil, yamux.ErrSessionShutdown
	}

	// Get session, if connected
	sess, err := c.session()
	if err != nil {
		return nil, err
	}

	// Open stream and wrap the stream Close method
	stream, err := sess.OpenStream()
	if err != nil {
		return nil, err
	}

	return newClientStream(c, stream), nil
}

func (c *ClientConnection) Close(ctx context.Context) {
	if !c.isClosed() {
		// Prevent new streams to be opened
		close(c.closed)

		c.lock.Lock()
		streams := maps.Values(c.streams)
		c.lock.Unlock()

		// Close streams
		wg := &sync.WaitGroup{}
		for _, s := range streams {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := s.Close(); err != nil {
					c.client.logger.Errorf(ctx, "disk writer client cannot close stream to %q (%d)", c.remoteAddr, s.StreamID())
				}
			}()
		}
		wg.Wait()

		// Close session
		if sess, _ := c.session(); sess != nil {
			select {
			case <-c.sess.CloseChan():
			default:
				if err := c.sess.Close(); err != nil {
					c.client.logger.Errorf(ctx, "disk writer client cannot close session to %q", c.remoteAddr)
				}
			}
		}

		// Wait for the dial loop
		c.wg.Wait()

		c.client.unregisterConnection(c)

		c.client.logger.Infof(ctx, `disk writer client closed connection to %q - %q`, c.remoteNodeID, c.remoteAddr)
	}
}

func (c *ClientConnection) dialLoop(ctx context.Context, initDone chan error) {
	b := newClientConnBackoff()
	for {
		if c.isClosed() || c.client.isClosed() {
			return
		}

		c.client.logger.Infof(ctx, `disk writer client is connecting to %q - %q`, c.remoteNodeID, c.remoteAddr)

		// Create session
		sess, err := c.newSession()

		// Update internal state
		c.lock.Lock()
		c.sess = sess
		c.lastError = err
		c.lock.Unlock()

		// Finish initialization after the first connection attempt
		if initDone != nil {
			if err != nil {
				initDone <- err
				close(initDone)
				return
			}
			close(initDone)
			initDone = nil
		}

		// Handle connection error with backoff
		if err != nil {
			delay := b.NextBackOff()
			c.client.logger.Errorf(ctx, `disk writer client cannot connect to %q - %q: %s, waiting %s before retry`, c.remoteNodeID, c.remoteAddr, err, delay)
			<-time.After(delay)
			continue
		}

		c.client.logger.Infof(ctx, `disk writer client connected from %q to %q - %q`, sess.LocalAddr().String(), c.remoteNodeID, c.remoteAddr)
		b.Reset()

		// Block while the connection is open
		<-sess.CloseChan()

		c.client.logger.Infof(ctx, `disk writer client disconnected from %q - %q`, c.remoteNodeID, c.remoteAddr)
	}
}

func (c *ClientConnection) newSession() (sess *yamux.Session, err error) {
	// Create connection
	conn, err := c.client.transport.Dial(c.remoteAddr)
	if err != nil {
		return nil, errors.PrefixErrorf(err, "cannot dial connection to %q -%q", c.remoteNodeID, c.remoteAddr)
	}

	// Create multiplexer
	sess, err = yamux.Client(conn, multiplexerConfig(c.client.logger, c.client.config))
	if err != nil {
		_ = conn.Close()
		return nil, errors.PrefixError(err, "cannot create client multiplexer")
	}

	return sess, nil
}

func (c *ClientConnection) session() (*yamux.Session, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.sess == nil {
		if c.lastError != nil {
			return nil, errors.PrefixError(c.lastError, "cannot open stream: no connection")
		}
		return nil, errors.New("cannot open stream: no connection")
	}

	return c.sess, nil
}

func (c *ClientConnection) registerStream(stream *ClientStream) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.streams[stream.StreamID()] = stream
}

func (c *ClientConnection) unregisterStream(stream *ClientStream) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.streams, stream.StreamID())
}

func (c *ClientConnection) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}

func newClientConnBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.2
	b.Multiplier = 2
	b.InitialInterval = 5 * time.Millisecond
	b.MaxInterval = 5 * time.Second
	b.MaxElapsedTime = 0 // don't stop
	b.Reset()
	return b
}
