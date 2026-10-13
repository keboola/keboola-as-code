package idletimer

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/dynamic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const writeTimeout = 5 * time.Second

// writeRetryInterval holds off a write that failed for a reason a retry may
// clear. Short, because the margin that covers a missed write is one heartbeat.
const writeRetryInterval = time.Second

// Recorder keeps the record of when each workload was last used: in memory for
// this replica, and in the shared CR the other replica reads. It is all the
// request path needs, and it decides nothing about suspending.
type Recorder struct {
	// wg counts nudges that have been accepted but not yet handled, so a waiter
	// covers the write a nudge goes on to make.
	wg sync.WaitGroup
	// A websocket frame can arrive after Shutdown begins: hijacked connections
	// are not drained by the HTTP server. Admission closes under this lock, so
	// no nudge is ever accepted after Wait has started counting down.
	closeLock sync.RWMutex
	closed    bool
	// stopping ends the writers, and is closed only once every accepted nudge
	// has been handled.
	stopping chan struct{}

	clock  clockwork.Clock
	logger log.Logger
	client *client

	workloads *syncmap.SyncMap[k8sapp.WorkloadRef, activity]
}

// activity is one workload's record keeping. Everything here belongs to the
// Recorder: the cadence is a property of the record it writes, so nothing
// outside may reach in and set it.
type activity struct {
	// nudges wakes this workload's writer. It holds one: a second would tell
	// the writer nothing the first does not.
	nudges chan struct{}
	// stop ends the writer when the workload leaves the cache.
	stop chan struct{}

	lock sync.Mutex

	lastRequestAt time.Time
	// lastWriteAt is the last request time this replica successfully wrote.
	lastWriteAt time.Time
	// retryAfter holds off a write that just failed. A retry a nudge later is
	// what keeps a transient failure from costing a heartbeat of margin; one
	// per request is what turned a standing failure into a storm.
	retryAfter time.Time

	sandboxName string
	threshold   *time.Duration
}

func NewRecorder(ctx context.Context, d dependencies, k8sClient dynamic.Interface) *Recorder {
	r := newRecorder(
		ctx,
		d.Clock(),
		d.Logger().WithComponent("idletimer"),
		newClient(k8sClient, d.Config().K8s.AppsNamespace),
	)

	d.AppStateWatcher().OnWorkloadRemoved(r.Forget)

	d.Process().OnShutdown(func(ctx context.Context) {
		r.Shutdown(ctx)
	})

	return r
}

func newRecorder(ctx context.Context, clock clockwork.Clock, logger log.Logger, c *client) *Recorder {
	r := &Recorder{
		stopping: make(chan struct{}),
		clock:    clock,
		logger:   logger,
		client:   c,
	}

	// GetOrInit runs this once per workload, under the map lock, which is what
	// makes starting the workload's writer here safe.
	r.workloads = syncmap.New[k8sapp.WorkloadRef, activity](func(ref k8sapp.WorkloadRef) *activity {
		item := &activity{
			nudges: make(chan struct{}, 1),
			stop:   make(chan struct{}),
		}
		go r.writeLoop(ctx, ref, item)
		return item
	})

	return r
}

// RecordActivity marks a workload as in use. It runs in the GotConn callback
// and, per frame, in the websocket observer, so it only takes the workload's
// own lock and nudges its writer. Whether a write is owed, and what it carries,
// is the writer's to decide.
//
// The mark is stored here rather than carried on the channel because a nudge
// may be dropped. A dropped nudge is harmless: one is already queued, and the
// writer reads the mark rather than the nudge.
func (r *Recorder) RecordActivity(_ context.Context, ref k8sapp.WorkloadRef) {
	item := r.workloads.GetOrInit(ref)

	item.lock.Lock()
	item.lastRequestAt = r.clock.Now()
	item.lock.Unlock()

	r.closeLock.RLock()
	if r.closed {
		r.closeLock.RUnlock()
		return
	}
	r.wg.Add(1)
	r.closeLock.RUnlock()

	select {
	case item.nudges <- struct{}{}:
	default:
		r.wg.Done()
	}
}

// Track tells the Recorder which record a workload writes to and how often, and
// reports what this replica has seen of it.
//
// A new member is a new record, so the cadence starts over. Carrying it across
// would leave the new record unwritten while traffic flows, and the other
// replica, reading a record that never advances, would suspend early.
func (r *Recorder) Track(ref k8sapp.WorkloadRef, sandboxName string, threshold *time.Duration) time.Time {
	item := r.workloads.GetOrInit(ref)

	item.lock.Lock()
	defer item.lock.Unlock()

	item.threshold = threshold
	if item.sandboxName != sandboxName {
		item.sandboxName = sandboxName
		item.lastWriteAt = time.Time{}
		item.retryAfter = time.Time{}
	}

	return item.lastRequestAt
}

// Forget drops a workload that has left the cache, and stops its writer.
func (r *Recorder) Forget(ref k8sapp.WorkloadRef) {
	if item, found := r.workloads.Delete(ref); found {
		close(item.stop)
	}
}

// Shutdown waits for the writes in flight. They are detached from their
// requests, so nothing else would wait for them.
func (r *Recorder) Shutdown(ctx context.Context) {
	r.closeLock.Lock()
	r.closed = true
	r.closeLock.Unlock()

	r.logger.Info(ctx, "waiting for pending idle timer writes")
	r.wg.Wait()

	close(r.stopping)
}

// writeLoop owns the write side of one workload. Serial by construction, so a
// workload never has two writes in flight, while workloads never wait on each
// other.
func (r *Recorder) writeLoop(ctx context.Context, ref k8sapp.WorkloadRef, item *activity) {
	for {
		select {
		case <-item.nudges:
			r.writeIfDue(ctx, ref, item)
			r.wg.Done()
		case <-item.stop:
			// Eviction is the only exit that can strand nudges: Shutdown closes
			// stopping only once every one of them has been handled.
			r.drainNudges(item)
			return
		case <-r.stopping:
			return
		}
	}
}

// drainNudges accounts for the nudges a stopped writer will not act on, so a
// waiter does not hang on them.
func (r *Recorder) drainNudges(item *activity) {
	for {
		select {
		case <-item.nudges:
			r.wg.Done()
		default:
			return
		}
	}
}

func (r *Recorder) writeIfDue(ctx context.Context, ref k8sapp.WorkloadRef, item *activity) {
	item.lock.Lock()
	at := item.lastRequestAt
	name := item.sandboxName
	due := writeDue(item, at, r.clock.Now())
	item.lock.Unlock()

	if !due {
		return
	}

	writeCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), writeTimeout, errors.New("idle timer write timeout"))
	defer cancel()

	err := r.client.cas(writeCtx, name, at)

	item.lock.Lock()
	defer item.lock.Unlock()

	if err == nil {
		item.lastWriteAt = at
		item.retryAfter = time.Time{}
		return
	}

	item.retryAfter = r.clock.Now().Add(retryDelay(item, err))
	r.logger.Warnf(writeCtx, "failed to record activity for workload %q: %s", ref, err)
}

// retryDelay holds a failed write off for one interval, and for a whole
// heartbeat when retrying cannot fix it: an absent record is created by the
// round, and a denied one needs an RBAC change.
func retryDelay(item *activity, err error) time.Duration {
	if (k8serrors.IsNotFound(err) || k8serrors.IsForbidden(err)) && item.threshold != nil {
		return heartbeatInterval(*item.threshold)
	}
	return writeRetryInterval
}

// writeDue reports whether this workload owes a shared write. The caller holds
// the lock.
//
// A workload nothing has tracked yet has no record name and writes nothing; the
// round then creates its record at the moment it first sees it, starting a full
// window. The name and the threshold are set together, so a known name means a
// known threshold.
func writeDue(item *activity, at, now time.Time) bool {
	if item.sandboxName == "" {
		return false
	}
	if item.threshold == nil {
		return false
	}
	if now.Before(item.retryAfter) {
		return false
	}
	return at.Sub(item.lastWriteAt) > heartbeatInterval(*item.threshold)
}
