// Package distribution provides distribution of various keys/tasks between worker nodes.
//
// The package consists of:
// - Registration of a worker node in the cluster as an etcd key (with lease).
// - Discovering of other worker nodes in the cluster by the etcd Watch API.
// - Local decision and assignment of a key/task to a specific worker node (by a consistent hash/HashRing approach).
//
// # Key benefits
//
// - The node only watch of other node's registration/un-registration, which doesn't happen often.
// - Based on this, the node can quickly and locally determine owner node for a key/task.
// - It aims to reduce the risk of collision and minimizes load.
//
// # Atomicity
//
// - During watch propagation or lease timeout, individual nodes can have a different list of the active nodes.
// - This could lead to the situation, when 2+ nodes have ownership of a task at the same time.
// - Therefore, the task itself must be also protected by a transaction (version number validation).
//
// Read more:
// - https://etcd.io/docs/v3.5/learning/why/#notes-on-the-usage-of-lock-and-lease
// - "Actually, the lease mechanism itself doesn't guarantee mutual exclusion...."
//
// # Listeners
//
// Use Node.OnChangeListener method to create a listener for nodes distribution change events.
package distribution

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/lafikl/consistent"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Node struct {
	clock   clock.Clock
	logger  log.Logger
	proc    *servicectx.Process
	schema  *schema.Schema
	client  *etcd.Client
	session *concurrency.Session
	nodeID  string

	config    config
	nodes     *consistent.Consistent
	listeners *listeners
}

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	EtcdClient() *etcd.Client
	Schema() *schema.Schema
	Process() *servicectx.Process
}

func NewNode(d dependencies, opts ...Option) (*Node, error) {
	// Apply options
	c := defaultConfig()
	for _, o := range opts {
		o(&c)
	}

	// Create instance
	n := &Node{
		clock:  d.Clock(),
		logger: d.Logger().AddPrefix("[distribution]"),
		proc:   d.Process(),
		schema: d.Schema(),
		client: d.EtcdClient(),
		nodeID: d.Process().UniqueID(),
		nodes:  consistent.New(),
		config: c,
	}

	// Create etcd session
	var err error
	n.session, err = etcdclient.CreateConcurrencySession(n.logger, n.proc, n.client, c.ttlSeconds)
	if err != nil {
		return nil, err
	}

	// Register node
	if err := n.register(c.startupTimeout); err != nil {
		return nil, err
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	n.proc.OnShutdown(func() {
		n.logger.Info("received shutdown request")
		cancel()
		wg.Wait()
		n.unregister(c.shutdownTimeout)
		n.logger.Info("shutdown done")
	})

	// Create listeners handler
	n.listeners = newListeners(n.proc, n.clock, n.logger, n.config)

	// Watch for nodes
	if err := n.watch(ctx, wg); err != nil {
		return nil, err
	}

	// Reset events from the initialization
	n.listeners.Reset()

	return n, nil
}

// OnChangeListener returns a new listener, it contains channel C with streamed distribution change Events.
func (n *Node) OnChangeListener() *Listener {
	return n.listeners.add()
}

// Nodes method returns IDs of all known nodes.
func (n *Node) Nodes() []string {
	out := n.nodes.Hosts()
	sort.Strings(out)
	return out
}

// NodeFor returns ID of the key's owner node.
// The consistent.ErrNoHosts may occur if there is no node in the list.
func (n *Node) NodeFor(key string) (string, error) {
	return n.nodes.Get(key)
}

// MustGetNodeFor returns ID of the key's owner node.
// The method panic if there is no node in the list.
func (n *Node) MustGetNodeFor(key string) string {
	node, err := n.NodeFor(key)
	if err != nil {
		panic(err)
	}
	return node
}

// IsOwner method returns true, if the node is owner of the key.
// The consistent.ErrNoHosts may occur if there is no node in the list.
func (n *Node) IsOwner(key string) (bool, error) {
	node, err := n.NodeFor(key)
	if err != nil {
		return false, err
	}
	return node == n.nodeID, nil
}

// MustCheckIsOwner method returns true, if the node is owner of the key.
// The method panic if there is no node in the list.
func (n *Node) MustCheckIsOwner(key string) bool {
	is, err := n.IsOwner(key)
	if err != nil {
		panic(err)
	}
	return is
}

func (n *Node) onWatchEvent(rawEvent etcdop.Event) {
	var event Event
	switch rawEvent.Type {
	case etcdop.CreateEvent, etcdop.UpdateEvent:
		nodeID := string(rawEvent.Kv.Value)
		event = Event{
			Type:    EventTypeAdd,
			NodeID:  nodeID,
			Message: fmt.Sprintf(`found a new node "%s"`, nodeID),
		}
		n.nodes.Add(nodeID)
		n.logger.Infof(event.Message)
	case etcdop.DeleteEvent:
		nodeID := string(rawEvent.PrevKv.Value)
		event = Event{
			Type:    EventTypeRemove,
			NodeID:  nodeID,
			Message: fmt.Sprintf(`the node "%s" gone`, nodeID),
		}
		n.nodes.Remove(nodeID)
		n.logger.Infof(event.Message)
	default:
		panic(errors.Errorf(`unexpected event type "%s"`, rawEvent.Type.String()))
	}

	n.listeners.Notify(event)
}

func (n *Node) onWatchErr(err error) {
	n.logger.Errorf("watcher failed: %s", err)
}

// register node in the etcd prefix,
// Deregistration is ensured double: by OnShutdown callback and by the lease.
func (n *Node) register(timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(n.client.Ctx(), timeout)
	defer cancel()

	startTime := time.Now()
	n.logger.Infof(`registering the node "%s"`, n.nodeID)

	key := n.schema.Runtime().WorkerNodes().Active().IDs().Node(n.nodeID)
	if err := key.Put(n.nodeID, etcd.WithLease(n.session.Lease())).Do(ctx, n.client); err != nil {
		return errors.Errorf(`cannot register the node "%s": %w`, n.nodeID, err)
	}

	n.logger.Infof(`the node "%s" registered | %s`, n.nodeID, time.Since(startTime))
	return nil
}

func (n *Node) unregister(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	startTime := time.Now()
	n.logger.Infof(`unregistering the node "%s"`, n.nodeID)

	key := n.schema.Runtime().WorkerNodes().Active().IDs().Node(n.nodeID)
	if _, err := key.Delete().Do(ctx, n.client); err != nil {
		n.logger.Warnf(`cannot unregister the node "%s": %s`, n.nodeID, err)
	}

	n.logger.Infof(`the node "%s" unregistered | %s`, n.nodeID, time.Since(startTime))
}

// watch for other nodes.
func (n *Node) watch(ctx context.Context, wg *sync.WaitGroup) error {
	selfDiscovery := n.waitForSelfDiscovery(ctx, wg)

	pfx := n.schema.Runtime().WorkerNodes().Active().IDs()
	ch := pfx.GetAllAndWatch(ctx, n.client, n.onWatchErr, etcd.WithPrevKV(), etcd.WithCreatedNotify())
	initDone := make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()
		n.logger.Info("watching for other nodes")

		// Channel is closed on shutdown, so the context does not have to be checked
		for events := range ch {
			if err := events.InitErr; err != nil {
				initDone <- err
				close(initDone)
			} else if events.Created {
				close(initDone)
			}
			for _, event := range events.Events {
				n.onWatchEvent(event)
			}
		}
	}()

	// Wait for self-discovery
	if err := <-selfDiscovery; err != nil {
		return err
	}

	// Wait for initial sync
	return <-initDone
}
