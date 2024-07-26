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
}

type volumeData struct {
	ID   volume.ID
	Node *nodeData
}

type nodeData struct {
	ID      string
	Address volume.RemoteAddr
}

type dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	StorageRepository() *storageRepo.Repository
}

func NewManager(d dependencies, cfg network.Config, nodeID string) (*Manager, error) {
	m := &Manager{
		logger: d.Logger().WithComponent("storage.router.connections"),
	}

	// Create transport client
	var err error
	m.client, err = transport.NewClient(m.logger.WithComponent("client"), cfg, nodeID)
	if err != nil {
		return nil, err
	}

	// Graceful shutdown
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	d.Process().OnShutdown(func(_ context.Context) {
		m.logger.Info(ctx, "closing connections")

		// Stop mirroring
		cancel()
		wg.Wait()

		// Close connections
		if err := m.client.Close(); err != nil {
			m.logger.Error(ctx, err.Error())
		}

		m.logger.Info(ctx, "closed connections")
	})

	// Start active volumes mirroring, only necessary data is saved
	{
		m.volumes = etcdop.
			SetupMirror(
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
				wg.Add(1)
				defer wg.Done()
				m.updateConnections(ctx)
			}).
			BuildMirror()
		if err := <-m.volumes.StartMirroring(ctx, wg, m.logger); err != nil {
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
	return m.client.Connection(vol.Node.ID)
}

func (m *Manager) ConnectionToNode(nodeID string) (*transport.ClientConnection, bool) {
	return m.client.Connection(nodeID)
}

func (m *Manager) ConnectionsCount() int {
	return m.client.ConnectionsCount()
}

func (m *Manager) updateConnections(ctx context.Context) {
	m.logger.Infof(ctx, `the list of volumes has changed, updating connections`)

	activeNodes := m.writerNodes()

	// Detect new nodes - to open connection
	var toOpen []*nodeData
	{
		for _, node := range activeNodes {
			if _, found := m.client.Connection(node.ID); !found {
				m.logger.Debugf(ctx, "new disk writer node %q", node.ID)
				toOpen = append(toOpen, node)
			}
		}
		slices.SortStableFunc(toOpen, func(a, b *nodeData) int {
			return strings.Compare(a.ID, b.ID)
		})
	}

	// Detect inactive nodes - to close connection
	var toClose []*transport.ClientConnection
	{
		for _, conn := range m.client.Connections() {
			if _, found := activeNodes[conn.RemoteNodeID()]; !found {
				m.logger.Debugf(ctx, "disk writer node gone %q ", conn.RemoteNodeID())
				toClose = append(toClose, conn)
			}
		}
		slices.SortStableFunc(toClose, func(a, b *transport.ClientConnection) int {
			return strings.Compare(a.RemoteNodeID(), b.RemoteNodeID())
		})
	}

	// Make changes
	for _, conn := range toClose {
		if err := conn.Close(ctx); err != nil {
			m.logger.Errorf(ctx, "cannot close connection to %q - %q: %s", conn.RemoteNodeID(), conn.RemoteAddr(), err)
		}
	}
	for _, node := range toOpen {
		// Start dial loop, errors are logged
		if _, err := m.client.OpenConnection(ctx, node.ID, node.Address.String()); err != nil {
			m.logger.Errorf(ctx, "cannot open connection to %q - %q: %s", node.ID, node.Address, err)
		}
	}
}

// writerNodes returns all active writer nodes.
func (m *Manager) writerNodes() map[string]*nodeData {
	out := make(map[string]*nodeData)
	m.volumes.Atomic(func(t prefixtree.TreeReadOnly[*volumeData]) {
		t.WalkAll(func(key string, vol *volumeData) (stop bool) {
			out[vol.Node.ID] = vol.Node
			return false
		})
	})
	return out
}
