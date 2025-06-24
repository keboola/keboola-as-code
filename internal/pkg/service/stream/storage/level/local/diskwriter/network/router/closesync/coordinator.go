package closesync

import (
	"context"
	"sync"

	"github.com/sasha-s/go-deadlock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/op"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	// NoSourceNode is revision number used to describe an edge-case, when no source node is running, only a coordinator node is running.
	// So it is guaranteed that no source node writes to the slice and the check can be skipped.
	NoSourceNode = int64(-1)
)

type CoordinatorNode struct {
	logger log.Logger
	client *etcd.Client
	schema schema

	// revision reported by source nodes
	revisions *etcdop.MirrorMap[int64, string, int64]

	listenersLock deadlock.Mutex
	listenerID    int
	listeners     map[int]*listener
}

type listener struct {
	id     int
	minRev int64
	done   chan struct{}
}

func NewCoordinatorNode(d dependencies) (*CoordinatorNode, error) {
	n := &CoordinatorNode{
		client:    d.EtcdClient(),
		logger:    d.Logger().WithComponent("close-sync.coordinator"),
		schema:    newSchema(d.EtcdSerde()),
		listeners: make(map[int]*listener),
	}

	// Graceful shutdown
	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(_ context.Context) {
		n.logger.Infof(ctx, "closing close-sync coordinator node")
		cancel(errors.New("shutting down: close-sync coordinator node"))
		wg.Wait()
		n.logger.Infof(ctx, "closed close-sync coordinator node")
	})

	// Watch the prefix
	{
		n.revisions = etcdop.SetupMirrorMap[int64, string, int64](
			n.schema.prefix.GetAllAndWatch(ctx, n.client),
			func(key string, value int64) string { return key },
			func(key string, value int64, rawValue *op.KeyValue, oldValue *int64) int64 { return value },
		).
			WithOnUpdate(func(_ etcdop.MirrorUpdate) {
				n.invokeListeners()
			}).
			BuildMirror()
		if err := <-n.revisions.StartMirroring(ctx, wg, n.logger, d.Telemetry(), d.WatchTelemetryInterval()); err != nil {
			return nil, err
		}
	}

	return n, nil
}

func (n *CoordinatorNode) MinRevInUse() (out int64) {
	out = NoSourceNode
	n.revisions.ForEach(func(_ string, v int64) (stop bool) {
		if out == NoSourceNode || out > v {
			out = v
		}
		return false
	})
	return out
}

// WaitForRevision waits until all source nodes are synced to the required revision or the context is cancelled.
func (n *CoordinatorNode) WaitForRevision(ctx context.Context, minRev int64) error {
	if greaterOrEqual(n.MinRevInUse(), minRev) {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-n.WaitForRevisionChan(minRev):
		return nil
	}
}

// WaitForRevisionChan returns the channel that is closed when all source nodes are synced to the required revision.
func (n *CoordinatorNode) WaitForRevisionChan(minRev int64) <-chan struct{} {
	if greaterOrEqual(n.MinRevInUse(), minRev) {
		done := make(chan struct{})
		close(done)
		return done
	}

	return n.newListener(minRev).done
}

func (n *CoordinatorNode) newListener(minRev int64) *listener {
	n.listenersLock.Lock()
	defer n.listenersLock.Unlock()

	l := &listener{id: n.listenerID, minRev: minRev, done: make(chan struct{})}
	n.listeners[n.listenerID] = l
	n.listenerID++

	return l
}

func (n *CoordinatorNode) invokeListeners() {
	n.listenersLock.Lock()
	defer n.listenersLock.Unlock()

	r := n.MinRevInUse()

	for id, l := range n.listeners {
		if greaterOrEqual(r, l.minRev) {
			close(l.done)
			delete(n.listeners, id)
		}
	}
}

func greaterOrEqual(actual, minimum int64) bool {
	return actual == NoSourceNode || actual >= minimum
}
