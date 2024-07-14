package transport

import (
	"context"
	"sync"

	"github.com/hashicorp/yamux"
	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Client struct {
	logger    log.Logger
	config    network.Config
	nodeID    string
	transport Transport

	closed chan struct{}
	wg     sync.WaitGroup

	lock     sync.Mutex
	sessions map[string]*yamux.Session
	streams  map[string]*yamux.Stream
}

type clientDependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
}

func NewClient(d clientDependencies, config network.Config, nodeID string) (*Client, error) {
	transport, err := newTransport(config)
	if err != nil {
		return nil, err
	}

	c := &Client{
		logger:    d.Logger().WithComponent("storage.node.writer.network.client"),
		config:    config,
		nodeID:    nodeID,
		transport: transport,
		closed:    make(chan struct{}),
		sessions:  make(map[string]*yamux.Session),
		streams:   make(map[string]*yamux.Stream),
	}

	// Graceful shutdown
	d.Process().OnShutdown(func(ctx context.Context) {
		c.logger.Info(ctx, "closing disk writer client")

		// Prevent new connections and streams to be opened
		close(c.closed)

		c.lock.Lock()
		streams := maps.Values(c.streams)
		sessions := maps.Values(c.sessions)
		c.lock.Unlock()

		// Close all streams
		c.logger.Infof(ctx, "closing %d streams", len(streams))
		streamsWg := &sync.WaitGroup{}
		for _, stream := range streams {
			streamsWg.Add(1)
			go func() {
				defer streamsWg.Done()
				if err := stream.Close(); err != nil {
					err = errors.PrefixError(err, "cannot close client stream")
					c.logger.Error(ctx, err.Error())
				}
			}()
		}
		streamsWg.Wait()

		// Close all sessions
		c.logger.Infof(ctx, "closing %d sessions", len(sessions))
		sessWg := &sync.WaitGroup{}
		for _, sess := range sessions {
			sessWg.Add(1)
			go func() {
				defer sessWg.Done()
				if err := sess.Close(); err != nil {
					err = errors.PrefixError(err, "cannot close client session")
					c.logger.Error(ctx, err.Error())
				}
			}()
		}
		sessWg.Wait()

		// Wait for remaining goroutines
		c.logger.Info(ctx, "waiting for goroutines")
		c.wg.Wait()
		c.logger.Info(ctx, "closed disk writer client")
	})

	return c, nil
}

func (c *Client) ConnectTo(targetAddr string) (*ClientConnection, error) {
	return newClientConnection(targetAddr, c)
}

func (c *Client) registerSession(sess *yamux.Session) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.sessions[sessionKey(sess)] = sess
}

func (c *Client) unregisterSession(sess *yamux.Session) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.sessions, sessionKey(sess))
}

func (c *Client) registerStream(stream *yamux.Stream) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.streams[streamKey(stream)] = stream
}

func (c *Client) unregisterStream(stream *yamux.Stream) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.streams, streamKey(stream))
}

func (c *Client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
