// Package connection manages connections from the current source node to all disk writer nodes.
//
// Principle of functionality:
//   - Each disk writer node reports its own mounted volumes to the database.
//   - The connection Manager watches for writer volumes changes, so it knows about all on-line disk writer nodes.
//   - With every change in writers volumes, it is checked that a connection to each writer nodes exists.
//   - Each connection is independent and covered by infinite retries, see transport.Client.OpenConnection.
//   - Connections are used by the storage router to send data to individual slices (disk writer nodes).
package connection

import (
	"context"
	"slices"
	"strings"
	"sync"

	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/prefixtree"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/transport"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	storageRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model/repository"
)

// Manager manages connections from the current source node to all disk writer nodes.
type Manager struct {
	logger log.Logger

	// client opens connections to disk writer nodes.
	client *transport.Client

	// volumes field contains in-memory snapshot of all active disk writer volumes.
	// It is used to get info about active disk writers, to open/close connections.
	volumes *etcdop.Mirror[volume.Metadata, *volumeData]

	closed <-chan struct{}
	wg     sync.WaitGroup

	connectionsLock sync.Mutex
	connections     map[string]*connection
}

type volumeData struct {
	ID   volume.ID
	Node *nodeData
}

type nodeData struct {
	ID      string
	Address volume.RemoteAddr
}

type connection struct {
	Node       *nodeData
	ClientConn *transport.ClientConnection
}

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	StorageRepository() *storageRepo.Repository
}

func NewManager(d dependencies, cfg network.Config, nodeID string) (*Manager, error) {
	m := &Manager{
		logger:      d.Logger().WithComponent("storage.router.connections"),
		connections: make(map[string]*connection),
	}

	ctx, cancel := context.WithCancel(context.Background())
	m.closed = ctx.Done()

	// Graceful shutdown
	d.Process().OnShutdown(func(_ context.Context) {
		m.logger.Info(ctx, "closing connections")
		cancel()
		m.wg.Wait()
		m.closeAllConnections(ctx)
		m.logger.Info(ctx, "closed connections")
	})

	// Create transport client
	var err error
	m.client, err = transport.NewClient(d, cfg, nodeID)
	if err != nil {
		return nil, err
	}

	// Start active volumes mirroring, only necessary data is saved
	{
		m.volumes = etcdop.
			SetupMirror(
				m.logger,
				d.StorageRepository().Volume().GetAllWriterVolumesAndWatch(ctx, etcd.WithPrevKV()),
				func(kv *op.KeyValue, vol volume.Metadata) string {
					return vol.ID.String()
				},
				func(kv *op.KeyValue, vol volume.Metadata) *volumeData {
					return &volumeData{
						ID: vol.ID,
						Node: &nodeData{
							ID:      vol.NodeID,
							Address: vol.NodeAddress,
						},
					}
				},
			).
			WithOnUpdate(func(_ etcdop.MirrorUpdate) {
				m.updateConnections(ctx)
			}).
			Build()
		if err := <-m.volumes.StartMirroring(ctx, &m.wg); err != nil {
			return nil, err
		}
	}

	return m, nil
}

func (m *Manager) ConnectionToVolume(volumeID volume.ID) (*transport.ClientConnection, bool) {
	vol, found := m.volumes.Get(volumeID.String())
	if !found {
		return nil, false
	}
	return m.ConnectionToNode(vol.Node.ID)
}

func (m *Manager) ConnectionToNode(nodeID string) (*transport.ClientConnection, bool) {
	m.connectionsLock.Lock()
	defer m.connectionsLock.Unlock()

	conn, found := m.connections[nodeID]
	if !found {
		return nil, false
	}

	return conn.ClientConn, true
}

func (m *Manager) OpenedConnectionsCount() int {
	m.connectionsLock.Lock()
	defer m.connectionsLock.Unlock()
	return len(m.connections)
}

func (m *Manager) updateConnections(ctx context.Context) {
	m.wg.Add(1)
	defer m.wg.Done()

	if m.isClosed() {
		return
	}

	m.logger.Infof(ctx, `the list of volumes has changed, updating connections`)

	activeNodes := m.writerNodes()

	m.connectionsLock.Lock()
	defer m.connectionsLock.Unlock()

	// Detect new nodes - to open connection
	var toOpen []*nodeData
	{
		for _, node := range activeNodes {
			if _, found := m.connections[node.ID]; !found {
				toOpen = append(toOpen, node)
			}
		}
		slices.SortStableFunc(toOpen, func(a, b *nodeData) int {
			return strings.Compare(a.ID, b.ID)
		})
		for _, node := range toOpen {
			m.openConnection(ctx, node)
		}
	}

	// Detect inactive nodes - to close connection
	var toClose []*connection
	{
		for _, conn := range m.connections {
			if _, found := activeNodes[conn.Node.Address]; !found {
				toClose = append(toClose, conn)
			}
		}
		slices.SortStableFunc(toClose, func(a, b *connection) int {
			return strings.Compare(a.Node.ID, b.Node.ID)
		})
		for _, conn := range toClose {
			m.closeConnection(ctx, conn)
		}
	}
}

func (m *Manager) openConnection(ctx context.Context, node *nodeData) {
	m.logger.Infof(ctx, `opening connection to %q - %q`, node.ID, node.Address)
	conn, err := m.client.OpenConnection(node.ID, node.Address.String())
	if err != nil {
		m.logger.Errorf(ctx, `cannot open connection to %q - %q`, node.ID, node.Address)
	}
	m.logger.Infof(ctx, `opened connection to %q - %q`, node.ID, node.Address)
	m.connections[node.ID] = &connection{
		Node:       node,
		ClientConn: conn,
	}
}

func (m *Manager) closeConnection(ctx context.Context, conn *connection) {
	delete(m.connections, conn.Node.ID)
	m.logger.Infof(ctx, `closing connection to %q - %q`, conn.Node.ID, conn.Node.Address)
	if err := conn.ClientConn.Close(); err != nil {
		m.logger.Errorf(ctx, `cannot close connection to %q - %q: %s`, conn.Node.ID, conn.Node.Address, err)
	}
	m.logger.Infof(ctx, `closed connection to %q - %q`, conn.Node.ID, conn.Node.Address)
}

func (m *Manager) closeAllConnections(ctx context.Context) {
	m.connectionsLock.Lock()
	defer m.connectionsLock.Unlock()
	wg := &sync.WaitGroup{}
	for _, conn := range m.connections {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m.closeConnection(ctx, conn)
		}()
	}
	wg.Wait()
}

// writerNodes returns all active writer nodes.
func (m *Manager) writerNodes() map[volume.RemoteAddr]*nodeData {
	out := make(map[volume.RemoteAddr]*nodeData)
	m.volumes.Atomic(func(t prefixtree.TreeReadOnly[*volumeData]) {
		t.WalkAll(func(key string, vol *volumeData) (stop bool) {
			out[vol.Node.Address] = vol.Node
			return false
		})
	})
	return out
}

func (m *Manager) isClosed() bool {
	select {
	case <-m.closed:
		return true
	default:
		return false
	}
}
