// Package apinode provides a configuration cache of receivers and slices for an API node.
// This speeds up the import endpoint, since no query to the etcd is needed.
// See documentation in the "watcher" package.
package apinode

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/schema"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/prefixtree"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Node contains cached state of all configuration objects required by the import endpoint, see GetReceiver method.
type Node struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	clock  clock.Clock
	logger log.Logger
	client *etcd.Client
	stats  *statistics.CollectorNode

	revision  *RevisionSyncer
	receivers *stateOf[model.ReceiverBase]
	slices    *stateOf[model.Slice]
}

// ReceiverCore is simplified version of the receiver, it contains only the data required by the import endpoint.
type ReceiverCore struct {
	model.ReceiverBase
	Slices []model.Slice
}

type Dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	Schema() *schema.Schema
	EtcdClient() *etcd.Client
	StatsCollector() *statistics.CollectorNode
}

type stateOf[T any] struct {
	*prefixtree.AtomicTree[T]
	initDone <-chan error
}

func New(d Dependencies, opts ...Option) (*Node, error) {
	proc, sm := d.Process(), d.Schema()

	// Apply options
	c := defaultConfig()
	for _, o := range opts {
		o(&c)
	}

	// Create
	n := &Node{
		clock:  d.Clock(),
		logger: d.Logger().AddPrefix("[api][watcher]"),
		client: d.EtcdClient(),
		stats:  d.StatsCollector(),
	}

	// Create etcd session
	sess, err := etcdclient.CreateConcurrencySession(n.logger, proc, d.EtcdClient(), c.ttlSeconds)
	if err != nil {
		return nil, err
	}

	// Graceful shutdown
	var cancel context.CancelFunc
	n.ctx, cancel = context.WithCancel(context.Background())
	n.wg = &sync.WaitGroup{}
	proc.OnShutdown(func() {
		n.logger.Info("received shutdown request")
		cancel()
		n.wg.Wait()
		n.logger.Info("shutdown done")
	})

	// Sync slices revision to Worker nodes
	nodeID := d.Process().UniqueID()
	revisionKey := sm.Runtime().APINodes().Watchers().Revision().Node(nodeID)
	n.revision, err = newSyncer(n.ctx, n.wg, n.clock, n.logger, n.stats, sess, revisionKey, c.syncInterval)
	if err != nil {
		return nil, err
	}

	// Watch receivers and slices
	n.receivers = watch(n, sm.Configs().Receivers().PrefixT(), nil)
	n.slices = watch(n, sm.Slices().Writing().PrefixT(), n.revision)

	// Wait for initial load
	startTime := time.Now()
	errs := errors.NewMultiError()
	if err := <-n.receivers.initDone; err != nil {
		errs.Append(err)
	}
	if err := <-n.slices.initDone; err != nil {
		errs.Append(err)
	}
	if errs.Len() == 0 {
		n.logger.Infof(`initialized | %s`, time.Since(startTime))
	}
	return n, errs.ErrorOrNil()
}

// StateRev returns current revision of the cached state.
// GetReceiver method return data in this revision.
func (s *Node) StateRev() int64 {
	return s.revision.StateRev()
}

// MinRevInUse returns minimum locked revision in use by a request.
// GetReceiver method locks the current revision
// and the returned UnlockFn callback unlocks the revision.
func (s *Node) MinRevInUse() int64 {
	return s.revision.MinRevInUse()
}

func (s *Node) GetReceiver(receiverKey key.ReceiverKey) (out ReceiverCore, found bool, unlockFn UnlockFn) {
	unlockFn = s.revision.Lock()

	// Get receiver
	out.ReceiverBase, found = s.receivers.Get(receiverKey.String())
	if !found {
		unlockFn()
		return out, false, nil
	}

	// Get opened slices
	slicePerExport := make(map[key.ExportKey]bool)
	for _, slice := range s.slices.AllFromPrefix(receiverKey.String()) {
		if slicePerExport[slice.ExportKey] {
			unlockFn()
			panic(errors.Errorf(`found multiple opened slices per export "%s"`, slice.ExportKey.String()))
		}
		slicePerExport[slice.ExportKey] = true
		out.Slices = append(out.Slices, slice)
	}

	return out, true, unlockFn
}

// The function belongs to the Node struct, but generic method cannot be currently defined.
func watch[T fmt.Stringer](n *Node, prefix etcdop.PrefixT[T], revSyncer *RevisionSyncer) *stateOf[T] {
	tree := prefixtree.New[T]()
	logsEnabled := false
	init := prefix.
		GetAllAndWatch(n.ctx, n.client, etcd.WithPrevKV()).
		SetupConsumer(n.logger).
		WithOnCreated(func(header *etcdop.Header) {
			logsEnabled = true
		}).
		WithForEach(func(events []etcdop.WatchEventT[T], header *etcdop.Header, restart bool) {
			tree.Atomic(func(t *prefixtree.Tree[T]) {
				// Reset the tree after receiving the first batch after the restart.
				if restart {
					t.Reset()
				}

				// Atomically process all events
				for _, event := range events {
					k := event.Value.String()
					switch event.Type {
					case etcdop.CreateEvent:
						t.Insert(k, event.Value)
						if logsEnabled {
							n.logger.Infof(`created %s%s`, prefix.Prefix(), k)
						}
					case etcdop.UpdateEvent:
						t.Insert(k, event.Value)
						if logsEnabled {
							n.logger.Infof(`updated %s%s`, prefix.Prefix(), k)
						}
					case etcdop.DeleteEvent:
						t.Delete(k)
						if logsEnabled {
							n.logger.Infof(`deleted %s%s`, prefix.Prefix(), k)
						}
					default:
						panic(errors.Errorf(`unexpected event type "%v"`, event.Type))
					}
				}
			})

			// ACK revision, so worker nodes knows that the API node is switched to the new slice.
			if revSyncer != nil {
				n.logger.Infof(`state updated to the revision "%v"`, header.Revision)
				revSyncer.Notify(header.Revision)
			}
		}).
		StartConsumer(n.wg)
	return &stateOf[T]{tree, init}
}
