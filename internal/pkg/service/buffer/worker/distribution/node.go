package distribution

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Node is created within each Worker node.
//
// It is responsible for:
// - Registration/un-registration of the worker node in the cluster, see register and unregister methods.
// - Discovery of the self and other nodes in the cluster, see watch method.
// - StartExecutor method starts a new Executor, which is restarted on the distribution changes.
// - Embedded assigner locally assigns the owner for the task, see documentation of the Assigner.
// - Embedded listeners listen for distribution changes, when a node is added or removed.
type Node struct {
	*assigner
	clock     clock.Clock
	logger    log.Logger
	proc      *servicectx.Process
	schema    *schema.Schema
	client    *etcd.Client
	session   *concurrency.Session
	config    nodeConfig
	listeners *listeners
}

type assigner = Assigner

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	EtcdClient() *etcd.Client
	Schema() *schema.Schema
	Process() *servicectx.Process
}

func NewNode(d dependencies, opts ...NodeOption) (*Node, error) {
	// Apply options
	c := defaultNodeConfig()
	for _, o := range opts {
		o(&c)
	}

	// Create instance
	n := &Node{
		assigner: newAssigner(d.Process().UniqueID()),
		clock:    d.Clock(),
		logger:   d.Logger().AddPrefix("[distribution]"),
		proc:     d.Process(),
		schema:   d.Schema(),
		client:   d.EtcdClient(),
		config:   c,
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
	n.listeners = newListeners(n)

	// Watch for nodes
	if err := n.watch(ctx, wg); err != nil {
		return nil, err
	}

	// Reset events created during the initialization.
	// There is no listener yet, and some events can be buffered by grouping interval.
	n.listeners.Reset()

	return n, nil
}

// OnChangeListener returns a new listener, it contains channel C with streamed distribution change Events.
func (n *Node) OnChangeListener() *Listener {
	return n.listeners.add()
}

// CloneAssigner returns cloned Assigner frozen in the actual distribution.
func (n *Node) CloneAssigner() *Assigner {
	return n.assigner.clone()
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
	pfx := n.schema.Runtime().WorkerNodes().Active().IDs()
	ch := pfx.GetAllAndWatch(ctx, n.client, etcd.WithPrevKV(), etcd.WithCreatedNotify())
	initDone := make(chan error)

	wg.Add(1)
	go func() {
		defer wg.Done()
		n.logger.Info("watching for other nodes")

		// Reset the nodes on the restart event.
		reset := false

		// Channel is closed on shutdown, so the context does not have to be checked
		for resp := range ch {
			switch {
			case resp.InitErr != nil:
				// Initialization error, stop worker via initDone channel
				initDone <- resp.InitErr
				close(initDone)
			case resp.Err != nil:
				// An error occurred, it is logged.
				// If it is a fatal error, then it is followed
				// by the "Restarted" event handled bellow,
				// and the operation starts from the beginning.
				n.logger.Error(resp.Err)
			case resp.Restarted:
				// A fatal error (etcd ErrCompacted) occurred.
				// It is not possible to continue watching, the operation must be restarted.
				reset = true
				n.logger.Warn(resp.RestartReason)
			case resp.Created:
				// The watcher has been successfully created.
				// This means transition from GetAll to Watch phase.
				close(initDone)
			default:
				events := n.updateNodesFrom(resp, reset)
				n.listeners.Notify(events)
				reset = false
			}
		}
	}()

	// Wait for initial sync
	if err := <-initDone; err != nil {
		return err
	}

	// Check self-discovery
	if !n.assigner.HasNode(n.nodeID) {
		return errors.Errorf(`self-discovery failed: missing "%s" in discovered nodes`, n.nodeID)
	}

	return nil
}

// updateNodesFrom events. The operation is atomic.
func (n *Node) updateNodesFrom(resp etcdop.WatchResponse, reset bool) Events {
	n.assigner.lock()
	defer n.assigner.unlock()

	if reset {
		n.assigner.resetNodes()
	}

	var events Events
	for _, rawEvent := range resp.Events {
		switch rawEvent.Type {
		case etcdop.CreateEvent, etcdop.UpdateEvent:
			nodeID := string(rawEvent.Kv.Value)
			event := Event{Type: EventNodeAdded, NodeID: nodeID, Message: fmt.Sprintf(`found a new node "%s"`, nodeID)}
			events = append(events, event)
			n.assigner.addNode(nodeID)
			n.logger.Infof(event.Message)
		case etcdop.DeleteEvent:
			nodeID := string(rawEvent.PrevKv.Value)
			event := Event{Type: EventNodeRemoved, NodeID: nodeID, Message: fmt.Sprintf(`the node "%s" gone`, nodeID)}
			events = append(events, event)
			n.assigner.removeNode(nodeID)
			n.logger.Infof(event.Message)
		default:
			panic(errors.Errorf(`unexpected event type "%s"`, rawEvent.Type.String()))
		}
	}
	return events
}
