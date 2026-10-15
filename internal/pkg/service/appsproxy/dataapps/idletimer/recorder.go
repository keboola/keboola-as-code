package idletimer

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/dynamic"

	"github.com/keboola/keboola-as-code/internal/pkg/idgenerator"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const writeTimeout = 5 * time.Second

// sweepInterval is the grain of the write side. It bounds how late a write can
// be, and the shortest heartbeat a threshold can ask for is three seconds.
const sweepInterval = time.Second

// sweepParallelism bounds how many writes one sweep has in flight, so a few
// slow ones cannot hold up the rest for a whole writeTimeout.
const sweepParallelism = 8

// writeRetryInterval holds off a write that failed for a reason a retry may
// clear. Short, because the margin that covers a missed write is one heartbeat.
const writeRetryInterval = time.Second

// Recorder keeps the record of when each workload was last used: in memory for
// this replica, and in the shared CR the other replica reads.
//
// A request only marks the workload. One sweeper writes what is owed, and a
// workload stays owed until a write for it succeeds, so a failure that outlives
// the traffic is still retried.
type Recorder struct {
	clock   clockwork.Clock
	logger  log.Logger
	client  *client
	metrics *recorderMetrics
	self    string

	workloads *syncmap.SyncMap[k8sapp.WorkloadRef, activity]
	errorLog  *errorLog

	stopOnce sync.Once
	stopping chan struct{}
	stopped  chan struct{}
}

type activity struct {
	lock sync.Mutex

	lastRequestAt time.Time
	// lastWriteAt is the last request time this replica successfully wrote.
	lastWriteAt time.Time
	// retryAfter holds off a write that just failed, so a standing failure
	// costs one attempt per interval rather than one per sweep.
	retryAfter time.Time
	// failures counts writes that have failed since the last one that landed.
	failures int

	// aliveAt and declared are this replica's entry as it last landed. A zero
	// declared means the record carries no entry for this replica yet.
	aliveAt  time.Time
	declared time.Duration
	// attempted is set by the first write to the record, landed or not.
	attempted bool
	// running is whether the last round saw the workload Running. Only then is
	// there anything for this replica's entry to vouch for.
	running bool

	sandboxName string
	threshold   *time.Duration
}

// NewRecorder starts the sweeper. The ctx the service is built with is not
// cancelled until the process has already exited, so the loop gets one of its
// own, cancelled after the final sweep rather than before it.
func NewRecorder(d dependencies, k8sClient dynamic.Interface) *Recorder {
	r := newRecorder(
		d.Clock(),
		d.Logger().WithComponent("idletimer"),
		newClient(k8sClient, d.Config().K8s.AppsNamespace),
		newRecorderMetrics(d.Telemetry().Meter()),
		newErrorLog(d.Clock(), d.Logger().WithComponent("idletimer")),
		instanceName(),
	)

	d.AppStateWatcher().OnWorkloadRemoved(r.Forget)

	ctx, cancel := context.WithCancelCause(context.Background())
	d.Process().OnShutdown(func(ctx context.Context) {
		r.Shutdown(ctx)
		cancel(errors.New("shutdown"))
	})

	go r.run(ctx)

	return r
}

func newRecorder(clock clockwork.Clock, logger log.Logger, c *client, metrics *recorderMetrics, errorLog *errorLog, self string) *Recorder {
	return &Recorder{
		clock:    clock,
		logger:   logger,
		client:   c,
		metrics:  metrics,
		errorLog: errorLog,
		self:     self,
		workloads: syncmap.New[k8sapp.WorkloadRef, activity](func(k8sapp.WorkloadRef) *activity {
			return &activity{}
		}),
		stopping: make(chan struct{}),
		stopped:  make(chan struct{}),
	}
}

// RecordActivity marks a workload as in use. It runs in the GotConn callback
// and, per frame, in the websocket observer, so it takes the workload's own
// lock and does nothing else. The sweeper decides what reaches the apiserver.
func (r *Recorder) RecordActivity(_ context.Context, ref k8sapp.WorkloadRef) {
	item := r.workloads.GetOrInit(ref)

	item.lock.Lock()
	defer item.lock.Unlock()

	item.lastRequestAt = r.clock.Now()
}

// observation is what this replica knows about a workload as a round reads it.
type observation struct {
	lastRequestAt time.Time
	// attempted reports whether this replica has tried to write the record yet.
	// Until it has, a missing entry means only that it has not, not that its
	// writes fail.
	attempted bool
}

// Observe folds this round's reading of a workload into the Recorder — which
// record it writes to, and how often — and reports what this replica has seen.
//
// A new member is a new record, so the cadence starts over. Carrying it across
// would leave the new record unwritten while traffic flows, and the other
// replica, reading a record that never advances, would suspend early.
func (r *Recorder) Observe(ref k8sapp.WorkloadRef, sandboxName string, threshold *time.Duration) observation {
	item := r.workloads.GetOrInit(ref)

	item.lock.Lock()
	defer item.lock.Unlock()

	item.threshold = threshold
	item.running = true
	if item.sandboxName != sandboxName {
		item.sandboxName = sandboxName
		item.lastWriteAt = time.Time{}
		item.retryAfter = time.Time{}
		item.aliveAt = time.Time{}
		item.declared = 0
		item.attempted = false
	}

	return observation{lastRequestAt: item.lastRequestAt, attempted: item.attempted}
}

// Retain marks every workload a round did not see Running as no longer
// running. A suspended or stopped workload stays in the cache, and writing its
// entry every heartbeat would vouch for nothing.
func (r *Recorder) Retain(running []k8sapp.WorkloadRef) {
	keep := make(map[k8sapp.WorkloadRef]bool, len(running))
	for _, ref := range running {
		keep[ref] = true
	}

	r.workloads.Range(func(ref k8sapp.WorkloadRef, item *activity) {
		if keep[ref] {
			return
		}
		item.lock.Lock()
		item.running = false
		item.lock.Unlock()
	})
}

// Forget drops a workload that has left the cache.
func (r *Recorder) Forget(ref k8sapp.WorkloadRef) {
	r.workloads.Delete(ref)
}

// Shutdown stops the sweeper after one final pass, so a mark taken just before
// the process goes down still reaches the shared record.
func (r *Recorder) Shutdown(ctx context.Context) {
	r.stopOnce.Do(func() { close(r.stopping) })

	r.logger.Info(ctx, "waiting for pending idle timer writes")

	select {
	case <-r.stopped:
	case <-ctx.Done():
		r.logger.Warnf(ctx, "gave up waiting for idle timer writes: %s", ctx.Err())
	}
}

func (r *Recorder) run(ctx context.Context) {
	defer close(r.stopped)

	ticker := r.clock.NewTicker(sweepInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.finalSweep(ctx)
			return
		case <-r.stopping:
			r.finalSweep(ctx)
			return
		case <-ticker.Chan():
			r.sweep(ctx)
		}
	}
}

// pendingWrite is what one sweep decided about one workload, read under its
// lock so the write itself needs none.
type pendingWrite struct {
	ref  k8sapp.WorkloadRef
	item *activity
	name string
	at   time.Time
	own  entry
	// leave takes this replica's entry out instead of writing it.
	leave bool
}

func (r *Recorder) sweep(ctx context.Context) {
	now := r.clock.Now()
	r.writeAll(ctx, r.collect(now, func(item *activity) bool {
		return writeDue(item, now)
	}))
}

// finalSweep writes every mark the record does not have yet, and takes this
// replica's entry out of every record that carries one. The cadence and the
// backoff both hold a mark for a later sweep, and after this one there is none.
// Once this replica is gone its heartbeat bounds nothing, and left in place it
// would widen every reader's margin until it went overdue.
//
// The marks go first. The shutdown has a deadline, and a leftover entry only
// goes overdue, while a lost mark is lost.
func (r *Recorder) finalSweep(ctx context.Context) {
	now := r.clock.Now()
	owed := r.collect(now, func(item *activity) bool {
		return item.sandboxName != "" && item.lastRequestAt.After(item.lastWriteAt)
	})
	rest := r.collect(now, func(item *activity) bool {
		return item.sandboxName != "" && !item.lastRequestAt.After(item.lastWriteAt) && item.declared != 0 && item.running
	})
	r.writeAll(ctx, leaving(owed))
	r.writeAll(ctx, leaving(rest))
}

func leaving(pending []pendingWrite) []pendingWrite {
	for i := range pending {
		pending[i].leave = true
	}
	return pending
}

// collect reads each workload's memory under its lock. That moment is the
// entry's aliveAt: a mark taken after it is not in the write.
func (r *Recorder) collect(now time.Time, owed func(item *activity) bool) []pendingWrite {
	var pending []pendingWrite
	r.workloads.Range(func(ref k8sapp.WorkloadRef, item *activity) {
		item.lock.Lock()
		defer item.lock.Unlock()

		if !owed(item) {
			return
		}
		pending = append(pending, pendingWrite{
			ref:  ref,
			item: item,
			name: item.sandboxName,
			at:   item.lastRequestAt,
			own:  entry{aliveAt: now, heartbeat: item.heartbeat()},
		})
	})
	return pending
}

func (r *Recorder) writeAll(ctx context.Context, pending []pendingWrite) {
	if len(pending) == 0 {
		return
	}

	g := &errgroup.Group{}
	g.SetLimit(sweepParallelism)
	for _, w := range pending {
		g.Go(func() error {
			r.write(ctx, w)
			return nil
		})
	}
	_ = g.Wait()
}

func (r *Recorder) write(ctx context.Context, w pendingWrite) {
	writeCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), writeTimeout, errors.New("idle timer write timeout"))
	defer cancel()

	var err error
	if w.leave {
		err = r.client.leave(writeCtx, w.name, r.self, w.at)
	} else {
		err = r.client.write(writeCtx, w.name, r.self, w.at, w.own)
	}

	w.item.lock.Lock()
	defer w.item.lock.Unlock()

	w.item.attempted = true
	if err != nil {
		w.item.failures++
		w.item.retryAfter = r.clock.Now().Add(retryDelay(w.item, err))
		r.metrics.writeFailures.Add(writeCtx, 1, withReason(k8serrors.ReasonForError(err)))
		r.errorLog.Warnf(writeCtx, err, "failed to record activity for workload %q: %s", w.ref, err)
		return
	}

	// A member switch during the write moved this workload to another record.
	// Claiming the cadence now would leave that one unwritten for a heartbeat.
	if w.item.sandboxName != w.name {
		return
	}

	w.item.lastWriteAt = w.at
	w.item.aliveAt = w.own.aliveAt
	w.item.declared = w.own.heartbeat
	if w.leave {
		w.item.aliveAt = time.Time{}
		w.item.declared = 0
	}
	w.item.retryAfter = time.Time{}
	w.item.failures = 0
}

// retryDelay holds a failed write off, doubling while it keeps failing. The
// ceiling is the workload's own heartbeat, so a failing write is never retried
// more often than a healthy one would be made. NotFound and Forbidden start at
// the ceiling, because no retry clears them: an absent record is created by the
// round, and a denied one needs an RBAC change.
func retryDelay(item *activity, err error) time.Duration {
	ceiling := item.heartbeat()

	if k8serrors.IsNotFound(err) || k8serrors.IsForbidden(err) {
		return ceiling
	}

	delay := writeRetryInterval
	for range max(item.failures-1, 0) {
		delay *= 2
		if delay >= ceiling {
			return ceiling
		}
	}
	return delay
}

// writeDue reports whether this workload owes a shared write. The caller holds
// the lock.
//
// A workload nothing has tracked yet has no record name and writes nothing; the
// round then creates its record at the moment it first sees it, starting a full
// window.
//
// A changed heartbeat is written at once. The reader applies the declared one,
// so a longer heartbeat must be declared before this replica lets the record
// trail by it, and a shorter one must land together with the trail the longer
// one allowed.
func writeDue(item *activity, now time.Time) bool {
	if item.sandboxName == "" || now.Before(item.retryAfter) {
		return false
	}

	heartbeat := item.heartbeat()
	if item.lastRequestAt.Sub(item.lastWriteAt) > heartbeat {
		return true
	}
	if !item.running {
		return false
	}
	return item.declared != heartbeat || now.Sub(item.aliveAt) >= heartbeat
}

// heartbeat is the cadence this replica writes a workload's record at. With no
// threshold it still writes, at the longest cadence, so that the record is
// current the moment a threshold is set.
func (a *activity) heartbeat() time.Duration {
	if a.threshold == nil {
		return maxHeartbeatInterval
	}
	return heartbeatInterval(*a.threshold)
}

// instanceName names this replica's entry. A pod's hostname is its name.
func instanceName() string {
	if name, err := os.Hostname(); err == nil && name != "" {
		return name
	}
	return "apps-proxy-" + idgenerator.Random(8)
}
