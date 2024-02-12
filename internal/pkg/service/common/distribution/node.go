package distribution

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
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

func NewNode(nodeID, group string, d dependencies, opts ...NodeOption) (*Node, error) {
	// Validate
	if nodeID == "" {
		panic(errors.New("distribution.Node: node ID cannot be empty"))
	}
	if group == "" {
		panic(errors.New("distribution.Node: group cannot be empty"))
	}

	// Apply options
	c := defaultNodeConfig()
	for _, o := range opts {
		o(&c)
	}

	// Create instance
	n := &Node{
		assigner:    newAssigner(nodeID),
		groupPrefix: etcdop.NewPrefix(fmt.Sprintf("runtime/distribution/group/%s/nodes", group)),
		clock:       d.Clock(),
		logger:      d.Logger().WithComponent("distribution." + group),
		proc:        d.Process(),
		client:      d.EtcdClient(),
		config:      c,
	}

	// Graceful shutdown
	bgContext := ctxattr.ContextWith(context.Background(), attribute.String("node", nodeID)) // nolint: contextcheck
	watchCtx, watchCancel := context.WithCancel(bgContext)
	sessionCtx, sessionCancel := context.WithCancel(bgContext)
	wg := &sync.WaitGroup{}
	n.proc.OnShutdown(func(ctx context.Context) {
		ctx = ctxattr.ContextWith(ctx, attribute.String("node", n.nodeID))
		n.logger.Info(ctx, "received shutdown request")
		watchCancel()
		n.unregister(ctx, c.shutdownTimeout)
		sessionCancel()
		wg.Wait()
		n.logger.Info(ctx, "shutdown done")
	})

	// Log node ID
	n.logger.Infof(watchCtx, `node ID "%s"`, nodeID)

	// Register node
	_, errCh := etcdop.
		NewSessionBuilder().
		WithTTLSeconds(c.ttlSeconds).
		WithOnSession(func(session *concurrency.Session) error { return n.register(session, c.startupTimeout) }).
		Start(sessionCtx, wg, n.logger, n.client)
	if err := <-errCh; err != nil {
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

	ctx = ctxattr.ContextWith(ctx, attribute.String("node", n.nodeID))

	startTime := time.Now()
	n.logger.Infof(ctx, `registering the node "%s"`, n.nodeID)

	key := n.groupPrefix.Key(n.nodeID)
	if err := key.Put(session.Client(), n.nodeID, etcd.WithLease(session.Lease())).Do(ctx).Err(); err != nil {
		return errors.Errorf(`cannot register the node "%s": %w`, n.nodeID, err)
	}

	n.logger.WithDuration(time.Since(startTime)).Infof(ctx, `the node "%s" registered`, n.nodeID)
	return nil
}

func (n *Node) unregister(ctx context.Context, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	startTime := time.Now()
	n.logger.Infof(ctx, `unregistering the node "%s"`, n.nodeID)

	key := n.groupPrefix.Key(n.nodeID)
	if err := key.Delete(n.client).Do(ctx).Err(); err != nil {
		n.logger.Warnf(ctx, `cannot unregister the node "%s": %s`, n.nodeID, err)
	}

	n.logger.WithDuration(time.Since(startTime)).Infof(ctx, `the node "%s" unregistered`, n.nodeID)
}

// watch for other nodes.
func (n *Node) watch(ctx context.Context, wg *sync.WaitGroup) error {
	n.logger.Info(ctx, "watching for other nodes")
	init := n.groupPrefix.
		GetAllAndWatch(ctx, n.client, etcd.WithPrevKV()).
		SetupConsumer(n.logger).
		WithForEach(func(events []etcdop.WatchEvent, _ *etcdop.Header, restart bool) {
			modifiedNodes := n.updateNodesFrom(ctx, events, restart)
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
func (n *Node) updateNodesFrom(ctx context.Context, events []etcdop.WatchEvent, reset bool) Events {
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
			n.logger.Infof(ctx, event.Message)
		case etcdop.DeleteEvent:
			nodeID := string(rawEvent.PrevKv.Value)
			event := Event{Type: EventNodeRemoved, NodeID: nodeID, Message: fmt.Sprintf(`the node "%s" gone`, nodeID)}
			out = append(out, event)
			n.assigner.removeNode(nodeID)
			n.logger.Infof(ctx, event.Message)
		default:
			panic(errors.Errorf(`unexpected event type "%s"`, rawEvent.Type.String()))
		}
	}
	return out
}
