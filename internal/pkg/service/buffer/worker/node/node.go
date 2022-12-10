// Package node provides:
// - Registration of the worker node in the cluster.
// - Discovering of other worker nodes in the cluster.
// - Assignment of a task to a specific worker node (by a consistent hash/HashRing approach).
//
// Key benefits:
//   - The node only watch of other node's registration/un-registration, which doesn't happen often.
//   - Based on this, the node can quickly and locally determine owner node for a task.
//   - It aims to reduce the risk of collision and minimizes load.
//
// Atomicity:
// - During watch propagation or lease timeout, individual nodes can have a different list of the active nodes.
// - This could lead to the situation, when 2+ nodes have ownership of a task at the same time.
// - Therefore, the task itself must be also protected by a transaction (version number validation).
//
// Read more:
// - https://etcd.io/docs/v3.5/learning/why/#notes-on-the-usage-of-lock-and-lease
// - "Actually, the lease mechanism itself doesn’t guarantee mutual exclusion...."
package node

import (
	"context"
	"sort"
	"time"

	"github.com/lafikl/consistent"
	"go.etcd.io/etcd/api/v3/mvccpb"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Node struct {
	config  config
	proc    *servicectx.Process
	logger  log.Logger
	schema  *schema.Schema
	client  *etcd.Client
	session *concurrency.Session

	id    string
	nodes *consistent.Consistent
}

type dependencies interface {
	Logger() log.Logger
	EtcdClient() *etcd.Client
	Schema() *schema.Schema
	Process() *servicectx.Process
}

func New(d dependencies, opts ...Option) (*Node, error) {
	// Apply options
	c := defaultConfig()
	for _, o := range opts {
		o(&c)
	}

	// Create instance
	n := &Node{
		config: c,
		proc:   d.Process(),
		logger: d.Logger(),
		schema: d.Schema(),
		client: d.EtcdClient(),
		id:     d.Process().UniqueID(),
		nodes:  consistent.New(),
	}

	// Create etcd session
	if err := n.createSession(); err != nil {
		return nil, err
	}

	// Register node
	if err := n.register(); err != nil {
		return nil, err
	}

	// Watch for nodes
	n.watch()

	return n, nil
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
	return node == n.id, nil
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

func (n *Node) onWatchEvent(event *etcd.Event) {
	switch event.Type {
	case mvccpb.PUT:
		nodeID := string(event.Kv.Value)
		n.nodes.Add(nodeID)
		n.logger.Infof(`found a new node "%s"`, nodeID)
	case mvccpb.DELETE:
		nodeID := string(event.PrevKv.Value)
		n.nodes.Remove(nodeID)
		n.logger.Infof(`the node "%s" gone`, nodeID)
	default:
		panic(errors.Errorf(`unexpected event type "%s"`, event.Type.String()))
	}
}

func (n *Node) createSession() (err error) {
	n.session, err = concurrency.NewSession(n.client, concurrency.WithTTL(n.config.ttlSeconds))
	if err != nil {
		return err
	}

	n.proc.OnShutdown(func() {
		if err := n.session.Close(); err != nil {
			n.logger.Warnf("cannot close etcd session: %s", err)
		} else {
			n.logger.Info("closed etcd session")
		}
	})

	n.logger.Info("created etcd session")
	return nil
}

// register node in the etcd prefix,
// Deregistration is ensured double: by OnShutdown callback and by the lease.
func (n *Node) register() error {
	ctx, cancel := context.WithTimeout(n.client.Ctx(), n.config.initTimeout)
	defer cancel()

	n.logger.Infof(`registering the node "%s"`, n.id)

	key := n.schema.Runtime().Workers().Active().IDs().Node(n.id)
	if err := key.Put(n.id, etcd.WithLease(n.session.Lease())).Do(ctx, n.client); err != nil {
		return errors.Errorf(`cannot register the node "%s": %w`, n.id, err)
	}

	n.proc.OnShutdown(func() {
		n.unregister()
	})

	n.logger.Infof(`the node "%s" registered`, n.id)
	return nil
}

func (n *Node) unregister() {
	ctx, cancel := context.WithTimeout(context.Background(), n.config.shutdownTimeout)
	defer cancel()

	n.logger.Infof(`unregistering the node "%s"`, n.id)

	key := n.schema.Runtime().Workers().Active().IDs().Node(n.id)
	if _, err := key.Delete().Do(ctx, n.client); err != nil {
		n.logger.Warnf(`cannot unregister the node "%s": %s`, n.id, err)
	}

	n.logger.Infof(`the node "%s" unregistered`, n.id)
}

// watch for other nodes.
func (n *Node) watch() {
	ctx, cancel := context.WithCancel(n.client.Ctx())
	n.proc.OnShutdown(func() {
		cancel()
		n.logger.Info("cancelled watcher")
	})

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				n.logger.Infof(`watching for other nodes`)
				pfx := n.schema.Runtime().Workers().Active().IDs()
				ch := pfx.Watch(ctx, n.client, etcd.WithRev(1), etcd.WithPrevKV(), etcd.WithCreatedNotify())
				n.processEvents(ctx, ch)

				// Wait and try to create watcher again
				time.Sleep(time.Second)
			}
		}
	}()
}

func (n *Node) processEvents(ctx context.Context, ch <-chan etcd.WatchResponse) {
	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				n.logger.Info("watcher channel closed")
				return
			}

			if err := resp.Err(); err != nil {
				n.logger.Errorf("watcher failed: %s", err)
				return
			}

			for _, event := range resp.Events {
				n.onWatchEvent(event)
			}
		}
	}
}
