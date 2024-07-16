package transport

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/hashicorp/yamux"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type ClientConnection struct {
	client *Client
	closed chan struct{}

	remoteNodeID string
	remoteAddr   string

	lastError error

	lock sync.Mutex
	sess *yamux.Session
}

func newClientConnection(remoteNodeID, remoteAddr string, c *Client, initDone chan error) (*ClientConnection, error) {
	// Stop, if the client is closed
	if c.isClosed() {
		return nil, yamux.ErrSessionShutdown
	}

	ctx := ctxattr.ContextWith(
		context.Background(),
		attribute.String("nodeId", c.nodeID),
		attribute.String("remoteNodeID", remoteNodeID),
		attribute.String("remoteAddr", remoteAddr),
	)

	conn := &ClientConnection{
		client:       c,
		closed:       make(chan struct{}),
		remoteNodeID: remoteNodeID,
		remoteAddr:   remoteAddr,
	}

	// Dial connection
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		conn.dialLoop(ctx, initDone)
	}()

	return conn, nil
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
	return newClientStream(stream, c.client), nil
}

func (c *ClientConnection) Close() (err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.isClosed() {
		return nil
	}

	close(c.closed)

	if sess := c.sess; sess != nil {
		err = sess.Close()
		c.sess = nil
		c.lastError = nil
		c.client.unregisterSession(sess)
	}

	return err
}

func (c *ClientConnection) dialLoop(ctx context.Context, initDone chan error) {
	// Close connection session on shutdown
	defer func() {
		if !c.isClosed() {
			if err := c.Close(); err != nil {
				err := errors.PrefixErrorf(err, `cannot close connection %q - %q`, c.remoteNodeID, c.remoteAddr)
				c.client.logger.Error(ctx, err.Error())
			}
		}
	}()

	b := newClientConnBackoff()

	for {
		if c.isClosed() || c.client.isClosed() {
			return
		}

		// Create session
		sess, err := c.newSession()

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
			c.client.logger.Errorf(ctx, `%s, waiting %s before retry`, err, delay)
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
	defer func() {
		// Update internal state
		c.lock.Lock()
		defer c.lock.Unlock()
		c.sess = sess
		c.lastError = err

		if sess != nil {
			c.client.registerSession(sess)
		}
	}()

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
