// Package revision provides synchronization of the revision used by an API node to Worker nodes.
//
// If the API node is synchronized to a new revision, for example "123",
// then the API node can acknowledge it only after the work
// that uses an older revision, for example "100", is completed.
//
// Workflow:
// - The Syncer runs on all API nodes (because API nodes use cached state to improve speed of the import endpoint).
// - If the API node receives an update of the state, it calls Notify method.
// - If the API node starts an operation that depends on the cached state, it calls LockCurrentRevision method.
// - The LockCurrentRevision return an unlock callback.
// - If the operation is completed, the callback must be invoked.
// - Syncer internally counts how many times a revision is in use.
// - The value is incremented by the LockCurrentRevision method and decremented by the unlock callback.
// - The minimum version that is currently in use is regularly synchronized to the etcd, see sync method.
package revision

import (
	"context"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/spf13/cast"
	etcd "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Syncer struct {
	logger    log.Logger
	targetKey etcdop.Key
	client    *etcd.Client

	// session ensures that when an outage occurs,
	// the key is automatically deleted after TTL seconds,
	// so an unavailable API node does not block Worker nodes.
	session *concurrency.Session

	lock *sync.Mutex
	// currentRev is version of the cached state, it is set by the Notify method
	currentRev int64
	// syncedRev is the latest version reported by the Syncer to Worker nodes, by the etcd
	syncedRev int64
	// revInUse contains the actual number of uses for each revision
	revInUse map[int64]int
}

type UnlockFn func()

type dependencies interface {
	Clock() clock.Clock
	Logger() log.Logger
	Process() *servicectx.Process
	EtcdClient() *etcd.Client
}

func NewSyncer(d dependencies, targetKey etcdop.Key, opts ...Option) (*Syncer, error) {
	// Apply options
	c := defaultConfig()
	for _, o := range opts {
		o(&c)
	}

	proc := d.Process()

	// Create
	r := &Syncer{
		logger:     d.Logger().AddPrefix("[watcher][api][revision]"),
		targetKey:  targetKey,
		client:     d.EtcdClient(),
		lock:       &sync.Mutex{},
		currentRev: 1,
		syncedRev:  0,
		revInUse:   make(map[int64]int),
	}

	// Create etcd session
	if s, err := etcdclient.CreateConcurrencySession(r.logger, proc, r.client, c.ttlSeconds); err == nil {
		r.session = s
	} else {
		return nil, err
	}

	// Graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	proc.OnShutdown(func() {
		r.logger.Info("received shutdown request")
		cancel()
		wg.Wait()
		r.logger.Info("shutdown done")
	})

	// Initial sync
	if err := r.sync(ctx); err != nil {
		return nil, err
	}

	// Periodical sync
	wg.Add(1)
	go func() {
		defer wg.Done()

		syncTicker := d.Clock().Ticker(c.syncInterval)
		defer syncTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-syncTicker.C:
				if err := r.sync(ctx); err != nil && !errors.Is(err, context.Canceled) {
					r.logger.Errorf(`sync error: %s`, err)
				}
			}
		}
	}()

	return r, nil
}

// Notify of an update of the state revision.
// Method is called by the API node.
// Value will be synced to the etcd, when no older version is in use.
func (r *Syncer) Notify(v int64) {
	r.lock.Lock()
	r.currentRev = v
	r.lock.Unlock()
}

// LockCurrentRevision blocks revision sync until a dependent work is completed.
func (r *Syncer) LockCurrentRevision() UnlockFn {
	// Increment usage of the revision
	r.lock.Lock()
	currentRev := r.currentRev
	r.revInUse[currentRev] += 1
	r.lock.Unlock()

	// Decrement usage by the unlock callback
	return func() {
		r.lock.Lock()
		v := r.revInUse[currentRev] - 1
		if v > 0 {
			r.revInUse[currentRev] = v
		} else {
			delete(r.revInUse, currentRev)
		}
		r.lock.Unlock()
	}
}

// minRevInUse returns the minimal revision currently in use.
func (r *Syncer) minRevInUse() int64 {
	min := r.currentRev
	for rev := range r.revInUse {
		if rev < min {
			min = rev
		}
	}
	return min
}

// sync the minimal revision currently in use to the etcd.
// Using this mechanism, worker nodes can safely determine when a revision is synchronized on all API nodes.
func (r *Syncer) sync(ctx context.Context) error {
	// Compare local and synced value
	r.lock.Lock()
	minRevInUse, syncedRev := r.minRevInUse(), r.syncedRev
	r.lock.Unlock()
	if minRevInUse == syncedRev {
		// nop
		return nil
	}

	// Update etcd key
	if err := r.targetKey.Put(cast.ToString(minRevInUse), etcd.WithLease(r.session.Lease())).Do(ctx, r.client); err != nil {
		return err
	}

	// Done, update value
	r.lock.Lock()
	r.syncedRev = minRevInUse
	r.lock.Unlock()

	r.logger.Infof(`reported revision "%v"`, minRevInUse)
	return nil
}
