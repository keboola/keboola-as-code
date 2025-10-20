package transport

import (
	"context"
	"slices"
	"strings"
	"sync"

	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Client struct {
	logger    log.Logger
	config    network.Config
	nodeID    string
	transport Protocol

	closed chan struct{}

	lock        sync.RWMutex
	connections map[string]*ClientConnection
}

func NewClient(logger log.Logger, config network.Config, nodeID string, transport Protocol) *Client {
	return &Client{
		logger:      logger.WithComponent("transport"),
		config:      config,
		nodeID:      nodeID,
		transport:   transport,
		closed:      make(chan struct{}),
		connections: make(map[string]*ClientConnection),
	}
}

// OpenConnection starts a connection dial loop to the target address.
// The method does not return an error, if it fails to connect, it tries again.
// The error is returned only when the client is closed.
// If the connection is dropped late, it tries to reconnect, until the client or connection Close method is called.
func (c *Client) OpenConnection(ctx context.Context, remoteNodeID, remoteAddr string) (*ClientConnection, error) {
	return newClientConnection(ctx, remoteNodeID, remoteAddr, c, nil)
}

// OpenConnectionOrErr will try to connect to the address and return an error if it fails.
// If the connection is closed after the initialization, it tries to reconnect, until the client or connection Close method is called.
func (c *Client) OpenConnectionOrErr(ctx context.Context, remoteNodeID, remoteAddr string) (*ClientConnection, error) {
	initDone := make(chan error, 1)
	conn, err := newClientConnection(ctx, remoteNodeID, remoteAddr, c, initDone)
	if err != nil {
		return nil, err
	}

	if err := <-initDone; err != nil {
		return nil, err
	}

	return conn, nil
}

func (c *Client) Connection(remoteNodeID string) (*ClientConnection, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()
	conn, found := c.connections[remoteNodeID]
	return conn, found
}

func (c *Client) Connections() []*ClientConnection {
	c.lock.RLock()
	defer c.lock.RUnlock()
	out := maps.Values(c.connections)
	slices.SortStableFunc(out, func(a, b *ClientConnection) int {
		return strings.Compare(a.remoteNodeID, b.remoteNodeID)
	})
	return out
}

func (c *Client) ConnectionsCount() int {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return len(c.connections)
}

func (c *Client) Close() error {
	if c.isClosed() {
		return errors.New("client is already closed")
	}

	ctx := context.Background()
	c.logger.Info(ctx, "closing disk writer client")
	defer c.logger.Info(ctx, "closed disk writer client")

	// Prevent new connections and streams to be opened
	close(c.closed)

	// Close all connections
	c.closeAllConnections(ctx)
	return nil
}

func (c *Client) closeAllConnections(ctx context.Context) {
	c.lock.Lock()
	connections := maps.Values(c.connections)
	c.lock.Unlock()

	c.logger.Infof(ctx, "closing %d connections", len(connections))
	wg := &sync.WaitGroup{}
	for _, conn := range connections {
		wg.Go(func() {
			if !conn.isClosed() {
				if err := conn.Close(ctx); err != nil {
					c.logger.Error(ctx, err.Error())
				}
			}
		})
	}
	wg.Wait()
}

func (c *Client) registerConnection(ctx context.Context, conn *ClientConnection) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.connections[conn.remoteNodeID] = conn
	c.logger.Debugf(ctx, "registered connection to %q, total connections count %d", conn.RemoteNodeID(), len(c.connections))
}

func (c *Client) unregisterConnection(ctx context.Context, conn *ClientConnection) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.connections, conn.remoteNodeID)
	c.logger.Debugf(ctx, "unregistered connection to %q, total connections count %d", conn.RemoteNodeID(), len(c.connections))
}

func (c *Client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
