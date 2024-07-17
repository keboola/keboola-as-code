package transport

import (
	"context"
	"slices"
	"strings"
	"sync"

	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
)

type Client struct {
	logger    log.Logger
	config    network.Config
	nodeID    string
	transport Transport

	closed chan struct{}

	lock        sync.RWMutex
	connections map[string]*ClientConnection
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
		logger:      d.Logger().WithComponent("storage.node.writer.network.client"),
		config:      config,
		nodeID:      nodeID,
		transport:   transport,
		closed:      make(chan struct{}),
		connections: make(map[string]*ClientConnection),
	}

	// Graceful shutdown
	d.Process().OnShutdown(func(ctx context.Context) {
		c.logger.Info(ctx, "closing disk writer client")

		// Prevent new connections and streams to be opened
		close(c.closed)

		c.lock.Lock()
		connections := maps.Values(c.connections)
		c.lock.Unlock()

		// Close all connections
		c.logger.Infof(ctx, "closing %d connections", len(connections))
		wg := &sync.WaitGroup{}
		for _, conn := range connections {
			wg.Add(1)
			go func() {
				defer wg.Done()
				conn.Close(ctx)
			}()
		}
		wg.Wait()
		c.logger.Info(ctx, "closed disk writer client")
	})

	return c, nil
}

// OpenConnection starts a connection dial loop to the target address.
// The method does not return an error, if it fails to connect, it tries again.
// The error is returned only when the client is closed.
// If the connection is dropped late, it tries to reconnect, until the client or connection Close method is called.
func (c *Client) OpenConnection(ctx context.Context, remoteNodeID, remoteAddr string) (*ClientConnection, error) {
	return openClientConnection(ctx, remoteNodeID, remoteAddr, c, nil)
}

// OpenConnectionOrErr will try to connect to the address and return an error if it fails.
// If the connection is closed after the initialization, it tries to reconnect, until the client or connection Close method is called.
func (c *Client) OpenConnectionOrErr(ctx context.Context, remoteNodeID, remoteAddr string) (*ClientConnection, error) {
	initDone := make(chan error, 1)
	conn, err := openClientConnection(ctx, remoteNodeID, remoteAddr, c, initDone)
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

func (c *Client) registerConnection(conn *ClientConnection) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.connections[conn.remoteNodeID] = conn
}

func (c *Client) unregisterConnection(conn *ClientConnection) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.connections, conn.remoteNodeID)
}

func (c *Client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
