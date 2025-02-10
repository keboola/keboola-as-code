package distribution

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Node is factory for GroupNode.
type Node struct {
	id           string
	config       Config
	dependencies dependencies
	groups       map[string]*GroupNode
}

type nodeChange struct {
	eventType EventType
	f         func(nodeID string)
	message   string
}

// GroupNode is created within each node in the group.
// One physical cluster node can be present in multiple independent distribution groups.
//
// It is responsible for:
// - Registration/un-registration of the node in the cluster, see register and unregister methods.
// - Discovery of the self and other nodes in the cluster, see watch method.
// - Embedded Assigner locally assigns an owner for a key, see documentation of the Assigner.
// - Embedded listeners listen for distribution changes, when a node is added or removed.
type GroupNode struct {
	*assigner
	logger      log.Logger
	config      Config
	client      *etcd.Client
	groupPrefix etcdop.Prefix
	listeners   *listeners

	sameNodes      map[string]nodeChange
	duplicateNodes map[string]nodeChange
}

type assigner = Assigner

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	EtcdClient() *etcd.Client
	Process() *servicectx.Process
}

func NewNode(nodeID string, cfg Config, d dependencies) *Node {
	if nodeID == "" {
		panic(errors.New("distribution.Node: node ID cannot be empty"))
	}
	return &Node{id: nodeID, config: cfg, dependencies: d, groups: make(map[string]*GroupNode)}
}

func (n *Node) Group(group string) (*GroupNode, error) {
	if _, ok := n.groups[group]; ok {
		return nil, errors.Errorf(`group "%s" has already been initialized`, group)
	}

	g, err := newGroupMember(n.id, group, n.config, n.dependencies)
	if err != nil {
		return nil, err
	}

	n.groups[group] = g
	return g, nil
}

func newGroupMember(nodeID, groupID string, cfg Config, d dependencies) (*GroupNode, error) {
	// Validate
	if groupID == "" {
		return nil, errors.Errorf("distribution group cannot be empty")
	}

	ctx := ctxattr.ContextWith(
		context.Background(), // nolint: contextcheck
		attribute.String("distribution.node", nodeID),
		attribute.String("distribution.group", groupID),
	)

	// Create instance
	g := &GroupNode{
		assigner:       newAssigner(nodeID),
		logger:         d.Logger().WithComponent("distribution"),
		config:         cfg,
		client:         d.EtcdClient(),
		groupPrefix:    etcdop.NewPrefix(fmt.Sprintf("runtime/distribution/group/%s/nodes", groupID)),
		sameNodes:      make(map[string]nodeChange),
		duplicateNodes: make(map[string]nodeChange),
	}

	// Graceful shutdown
	watchCtx, watchCancel := context.WithCancel(ctx)
	sessionCtx, sessionCancel := context.WithCancel(ctx)
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func(ctx context.Context) {
		g.logger.Info(ctx, "received shutdown request")
		watchCancel()
		g.unregister(ctx, cfg.ShutdownTimeout)
		sessionCancel()
		wg.Wait()
		g.logger.Info(ctx, "shutdown done")
	})

	// Log node ID
	g.logger.Info(ctx, "joining distribution group")

	// Register node
	_, err := etcdop.
		NewSessionBuilder().
		WithGrantTimeout(cfg.GrantTimeout).
		WithTTLSeconds(cfg.TTLSeconds).
		WithOnSession(g.register).
		StartOrErr(sessionCtx, wg, g.logger, d.EtcdClient())
	if err != nil {
		return nil, err
	}

	// Create listeners handler
	g.listeners = newListeners(watchCtx, wg, cfg, g.logger, d)

	// Watch for nodes
	if err := g.watch(watchCtx, wg); err != nil {
		return nil, err
	}

	// Reset events created during the initialization.
	// There is no listener yet, but some events can be buffered by grouping interval.
	g.listeners.Reset()

	return g, nil
}

// OnChangeListener returns a new listener, it contains channel C with streamed distribution change Events.
func (n *GroupNode) OnChangeListener() *Listener {
	return n.listeners.add()
}

// CloneAssigner returns cloned Assigner frozen in the actual distribution.
func (n *GroupNode) CloneAssigner() *Assigner {
	return n.assigner.clone()
}

// register node in the etcd prefix,
// Un-registration is ensured double: by OnShutdown callback and by the lease.
func (n *GroupNode) register(session *concurrency.Session) error {
	ctx, cancel := context.WithTimeout(session.Client().Ctx(), n.config.StartupTimeout)
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

func (n *GroupNode) unregister(ctx context.Context, timeout time.Duration) {
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
func (n *GroupNode) watch(ctx context.Context, wg *sync.WaitGroup) error {
	n.logger.Info(ctx, "watching for other nodes")
	consumer := n.groupPrefix.
		GetAllAndWatch(ctx, n.client, etcd.WithPrevKV()).
		SetupConsumer().
		WithForEach(func(events []etcdop.WatchEvent[[]byte], _ *etcdop.Header, restart bool) {
			modifiedNodes := n.updateNodesFrom(ctx, events, restart)
			n.listeners.Notify(modifiedNodes)
		}).
		BuildConsumer()

	// Wait for initial sync
	if err := <-consumer.StartConsumer(ctx, wg, n.logger); err != nil {
		return err
	}

	// Check self-discovery
	if !n.assigner.HasNode(n.nodeID) {
		return errors.Errorf(`self-discovery failed: missing "%s" in discovered nodes`, n.nodeID)
	}

	return nil
}

// updateNodesFrom events. The operation is atomic.
func (n *GroupNode) updateNodesFrom(ctx context.Context, events []etcdop.WatchEvent[[]byte], reset bool) Events {
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
			if _, ok := n.sameNodes[nodeID]; ok {
				continue
			}
			event := Event{Type: EventNodeAdded, NodeID: nodeID, Message: fmt.Sprintf(`found a new node "%s"`, nodeID)}
			out = append(out, event)
			n.sameNodes[nodeID] = nodeChange{eventType: EventNodeAdded, f: n.assigner.addNode, message: event.Message}
		case etcdop.DeleteEvent:
			nodeID := string(rawEvent.PrevKv.Value)
			if _, ok := n.sameNodes[nodeID]; !ok {
				continue
			}
			event := Event{Type: EventNodeRemoved, NodeID: nodeID, Message: fmt.Sprintf(`the node "%s" gone`, nodeID)}
			out = append(out, event)
			n.sameNodes[nodeID] = nodeChange{eventType: EventNodeRemoved, f: n.assigner.removeNode, message: event.Message}
		default:
			panic(errors.Errorf(`unexpected event type "%s"`, rawEvent.Type.String()))
		}
	}

	for key, nodeChange := range n.sameNodes {
		nodeChange.f(key)
		if nodeChange.eventType == EventNodeRemoved {
			delete(n.sameNodes, key)
		}
		n.logger.Infof(ctx, nodeChange.message)
	}

	return out
}
