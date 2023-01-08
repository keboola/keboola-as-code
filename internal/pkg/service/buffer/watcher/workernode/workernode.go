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
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
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
	proc := d.Process()

	// Create
	n := &Node{
		logger:             d.Logger().AddPrefix("[watcher][worker]"),
		schema:             d.Schema(),
		client:             d.EtcdClient(),
		minRevision:        atomic.NewInt64(1),
		revisionPerAPINode: make(map[string]int64),
		listeners:          newListeners(),
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	proc.OnShutdown(func() {
		n.logger.Info("received shutdown request")
		cancel()
		wg.Wait()
		n.logger.Info("shutdown done")
	})

	// Watch revisions of all API nodes
	if err := n.watch(ctx, wg); err != nil {
		return nil, err
	}

	return n, nil
}

// WaitForRevision returns the channel that is closed when all API nodes are synced to the required revision.
func (n *Node) WaitForRevision(requiredRev int64) <-chan struct{} {
	return n.listeners.waitForRevision(requiredRev, n.minRevision)
}

// watch for changes in revisions of API nodes.
func (n *Node) watch(ctx context.Context, wg *sync.WaitGroup) error {
	pfx := n.schema.Runtime().APINodes().Watchers().SlicesRevision()
	ch, initDone := pfx.GetAllAndWatch(ctx, n.client, n.onError, etcd.WithCreatedNotify())

	wg.Add(1)
	go func() {
		defer wg.Done()

		// Channel is closed on shutdown, so the context does not have to be checked
		for events := range ch {
			for _, event := range events.Events {
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
			if rev := minimalRevision(n.revisionPerAPINode); rev != noAPINode {
				if old := n.minRevision.Swap(rev); old != rev {
					// Trigger listeners if the minimal version has changed
					if count := n.listeners.onChange(ctx, n.minRevision); count > 0 {
						n.logger.Infof(`revision updated to "%v", unblocked "%d" listeners`, rev, count)
					} else {
						n.logger.Infof(`revision updated to "%v"`, rev)
					}
				}
			}
		}
	}()

	// Wait for initial load
	return <-initDone
}

func (n *Node) onError(err error) {
	n.logger.Error(err)
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
