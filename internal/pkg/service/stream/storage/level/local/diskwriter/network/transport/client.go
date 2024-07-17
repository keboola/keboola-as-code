package transport

import (
	"context"
	"sync"

	"go.uber.org/atomic"
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

	connIDCounter *atomic.Uint64

	lock        sync.Mutex
	connections map[uint64]*ClientConnection
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
		logger:        d.Logger().WithComponent("storage.node.writer.network.client"),
		config:        config,
		nodeID:        nodeID,
		transport:     transport,
		closed:        make(chan struct{}),
		connIDCounter: atomic.NewUint64(0),
		connections:   make(map[uint64]*ClientConnection),
	}

	// Graceful shutdown
	d.Process().OnShutdown(func(ctx context.Context) {
		c.logger.Info(ctx, "closing disk writer client")

		// Prevent new connections and streams to be opened
		close(c.closed)

		c.lock.Lock()
		connections := maps.Values(c.connections)
		c.lock.Unlock()
		c.closeAllConnections(ctx, connections)
	})

	return c, nil
}

// OpenConnection starts a connection dial loop to the target address.
// The method does not return an error, if it fails to connect, it tries again.
// The error is returned only when the client is closed.
// If the connection is dropped late, it tries to reconnect, until the client or connection Close method is called.
func (c *Client) OpenConnection(remoteNodeID, remoteAddr string) (*ClientConnection, error) {
	return newClientConnection(c.connIDCounter.Inc(), remoteNodeID, remoteAddr, c, nil)
}

// OpenConnectionOrErr will try to connect to the address and return an error if it fails.
// If the connection is closed after the initialization, it tries to reconnect, until the client or connection Close method is called.
func (c *Client) OpenConnectionOrErr(remoteNodeID, remoteAddr string) (*ClientConnection, error) {
	initDone := make(chan error, 1)
	conn, err := newClientConnection(c.connIDCounter.Inc(), remoteNodeID, remoteAddr, c, initDone)
	if err != nil {
		return nil, err
	}

	if err := <-initDone; err != nil {
		return nil, err
	}

	return conn, nil
}

func (c *Client) registerConnection(conn *ClientConnection) {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.connections[conn.id] = conn
}

func (c *Client) unregisterConnection(conn *ClientConnection) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.connections, conn.id)
}

func (c *Client) closeAllConnections(ctx context.Context, connections []*ClientConnection) {
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
}

func (c *Client) isClosed() bool {
	select {
	case <-c.closed:
		return true
	default:
		return false
	}
}
