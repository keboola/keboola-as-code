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
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Node is created within each node in the group.
//
// It is responsible for:
// - Registration/un-registration of the node in the cluster, see register and unregister methods.
// - Discovery of the self and other nodes in the cluster, see watch method.
// - Embedded Assigner locally assigns a owner for a key, see documentation of the Assigner.
// - Embedded listeners listen for distribution changes, when a node is added or removed.
type Node struct {
	*assigner
	groupPrefix etcdop.Prefix
	clock       clock.Clock
	logger      log.Logger
	proc        *servicectx.Process
	client      *etcd.Client
	config      nodeConfig
	listeners   *listeners
}

type assigner = Assigner

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	EtcdClient() *etcd.Client
	Process() *servicectx.Process
}

func NewNode(group string, d dependencies, opts ...NodeOption) (*Node, error) {
	// Apply options
	c := defaultNodeConfig()
	for _, o := range opts {
		o(&c)
	}

	// Create instance
	n := &Node{
		assigner:    newAssigner(d.Process().UniqueID()),
		groupPrefix: etcdop.NewPrefix(fmt.Sprintf("runtime/distribution/group/%s/nodes", group)),
		clock:       d.Clock(),
		logger:      d.Logger().AddPrefix(fmt.Sprintf("[distribution][%s]", group)),
		proc:        d.Process(),
		client:      d.EtcdClient(),
		config:      c,
	}

	// Graceful shutdown
	watchCtx, watchCancel := context.WithCancel(context.Background())
	sessionCtx, sessionCancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	n.proc.OnShutdown(func(ctx context.Context) {
		n.logger.InfoCtx(ctx, "received shutdown request")
		watchCancel()
		n.unregister(c.shutdownTimeout)
		sessionCancel()
		wg.Wait()
		n.logger.InfoCtx(ctx, "shutdown done")
	})

	sessionInit := etcdop.ResistantSession(sessionCtx, wg, n.logger, n.client, c.ttlSeconds, func(session *concurrency.Session) error {
		// Register node
		return n.register(session, c.startupTimeout)
	})
	if err := <-sessionInit; err != nil {
		return nil, err
	}

	// Create listeners handler
	n.listeners = newListeners(n)

	// Watch for nodes
	if err := n.watch(watchCtx, wg); err != nil {
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
func (n *Node) register(session *concurrency.Session, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(n.client.Ctx(), timeout)
	defer cancel()

	startTime := time.Now()
	n.logger.InfofCtx(ctx, `registering the node "%s"`, n.nodeID)

	key := n.groupPrefix.Key(n.nodeID)
	if err := key.Put(n.nodeID, etcd.WithLease(session.Lease())).Do(ctx, session.Client()); err != nil {
		return errors.Errorf(`cannot register the node "%s": %w`, n.nodeID, err)
	}

	n.logger.InfofCtx(ctx, `the node "%s" registered | %s`, n.nodeID, time.Since(startTime))
	return nil
}

func (n *Node) unregister(timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	startTime := time.Now()
	n.logger.InfofCtx(ctx, `unregistering the node "%s"`, n.nodeID)

	key := n.groupPrefix.Key(n.nodeID)
	if _, err := key.Delete().Do(ctx, n.client); err != nil {
		n.logger.WarnfCtx(ctx, `cannot unregister the node "%s": %s`, n.nodeID, err)
	}

	n.logger.InfofCtx(ctx, `the node "%s" unregistered | %s`, n.nodeID, time.Since(startTime))
}

// watch for other nodes.
func (n *Node) watch(ctx context.Context, wg *sync.WaitGroup) error {
	n.logger.InfoCtx(ctx, "watching for other nodes")
	init := n.groupPrefix.
		GetAllAndWatch(ctx, n.client, etcd.WithPrevKV()).
		SetupConsumer(n.logger).
		WithForEach(func(events []etcdop.WatchEvent, _ *etcdop.Header, restart bool) {
			modifiedNodes := n.updateNodesFrom(events, restart)
			n.listeners.Notify(modifiedNodes)
		}).
		StartConsumer(ctx, wg)

	// Wait for initial sync
	if err := <-init; err != nil {
		return err
	}

	// Check self-discovery
	if !n.assigner.HasNode(n.nodeID) {
		return errors.Errorf(`self-discovery failed: missing "%s" in discovered nodes`, n.nodeID)
	}

	return nil
}

// updateNodesFrom events. The operation is atomic.
func (n *Node) updateNodesFrom(events []etcdop.WatchEvent, reset bool) Events {
	n.assigner.lock()
	defer n.assigner.unlock()

	if reset {
		n.assigner.resetNodes()
	}

	var out Events
	for _, rawEvent := range events {
		switch rawEvent.Type {
		case etcdop.CreateEvent, etcdop.UpdateEvent:
			nodeID := string(rawEvent.Kv.Value)
			event := Event{Type: EventNodeAdded, NodeID: nodeID, Message: fmt.Sprintf(`found a new node "%s"`, nodeID)}
			out = append(out, event)
			n.assigner.addNode(nodeID)
			n.logger.Infof(event.Message)
		case etcdop.DeleteEvent:
			nodeID := string(rawEvent.PrevKv.Value)
			event := Event{Type: EventNodeRemoved, NodeID: nodeID, Message: fmt.Sprintf(`the node "%s" gone`, nodeID)}
			out = append(out, event)
			n.assigner.removeNode(nodeID)
			n.logger.Infof(event.Message)
		default:
			panic(errors.Errorf(`unexpected event type "%s"`, rawEvent.Type.String()))
		}
	}
	return out
}
