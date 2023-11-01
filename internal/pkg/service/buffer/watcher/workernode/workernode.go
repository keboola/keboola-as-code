// Package workernode provides synchronization between API and Worker nodes.
// See documentation in the "watcher" package.
package workernode

import (
	"context"
	"sync"

	"github.com/spf13/cast"
	etcd "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const noAPINode int64 = -1

type Node struct {
	logger log.Logger
	schema *schema.Schema
	client *etcd.Client

	minRevision        *atomic.Int64
	revisionPerAPINode map[string]int64

	listeners *listeners
}

type Dependencies interface {
	Logger() log.Logger
	Process() *servicectx.Process
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
}

func New(d Dependencies) (*Node, error) {
	// Create
	n := &Node{
		logger:             d.Logger().AddPrefix("[watcher][worker]"),
		schema:             d.Schema(),
		client:             d.EtcdClient(),
		minRevision:        atomic.NewInt64(noAPINode),
		revisionPerAPINode: make(map[string]int64),
	}

	// Init listeners, all must be fulfilled on shutdown
	n.listeners = newListeners(n.logger)

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	d.Process().OnShutdown(func() {
		n.logger.Info("received shutdown request")
		n.listeners.wait()
		cancel()
		wg.Wait()
		n.logger.Info("shutdown done")
	})

	// Watch revisions of all API nodes
	if err := <-n.watch(ctx, wg); err != nil {
		return nil, err
	}

	return n, nil
}

func (n *Node) ListenersCount() int {
	return n.listeners.count()
}

// WaitForRevision waits until all API nodes are synced to the required revision or the context is cancelled.
func (n *Node) WaitForRevision(ctx context.Context, rev int64) error {
	wait, cancel := n.WaitForRevisionChan(rev)
	defer cancel()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-wait:
		return nil
	}
}

// WaitForRevisionChan returns the channel that is closed when all API nodes are synced to the required revision.
func (n *Node) WaitForRevisionChan(requiredRev int64) (<-chan struct{}, func()) {
	l := n.listeners.waitForRevision(requiredRev, n.minRevision)
	return l.C, l.Cancel
}

// watch for changes in revisions of API nodes.
func (n *Node) watch(ctx context.Context, wg *sync.WaitGroup) (init <-chan error) {
	return n.schema.
		Runtime().APINodes().Watchers().Revision().
		GetAllAndWatch(ctx, n.client).
		SetupConsumer(n.logger).
		WithForEach(func(events []etcdop.WatchEvent, header *etcdop.Header, restart bool) {
			n.updateRevisionsFrom(ctx, events, restart)
		}).
		StartConsumer(wg)
}

func (n *Node) updateRevisionsFrom(ctx context.Context, events []etcdop.WatchEvent, restart bool) {
	if restart {
		n.revisionPerAPINode = make(map[string]int64)
	}

	for _, event := range events {
		switch event.Type {
		case etcdop.CreateEvent, etcdop.UpdateEvent:
			// Cached state of th API node has been updated
			n.revisionPerAPINode[string(event.Kv.Key)] = cast.ToInt64(string(event.Kv.Value))
		case etcdop.DeleteEvent:
			// The API node gone
			delete(n.revisionPerAPINode, string(event.Kv.Key))
		default:
			panic(errors.Errorf(`unexpected event type "%s"`, event.Type.String()))
		}
	}

	// Recompute and store minimal revision
	rev := minimalRevision(n.revisionPerAPINode)
	if old := n.minRevision.Swap(rev); old != rev {
		// Trigger listeners if the minimal version has changed
		if count := n.listeners.onChange(ctx, n.minRevision); count > 0 {
			if rev == noAPINode {
				n.logger.Infof(`all API nodes are gone, unblocked "%d" listeners`, count)
			} else {
				n.logger.Infof(`revision updated to "%v", unblocked "%d" listeners`, rev, count)
			}
		} else {
			if rev == noAPINode {
				n.logger.Info(`all API nodes are gone`)
			} else {
				n.logger.Infof(`revision updated to "%v"`, rev)
			}
		}
	}
}

func minimalRevision(revisionPerAPINode map[string]int64) (min int64) {
	min = noAPINode
	for _, v := range revisionPerAPINode {
		if min == noAPINode || min > v {
			min = v
		}
	}
	return min
}
