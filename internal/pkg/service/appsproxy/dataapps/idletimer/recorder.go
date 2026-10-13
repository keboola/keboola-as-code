package idletimer

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	"golang.org/x/sync/errgroup"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/dynamic"

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

	workloads *syncmap.SyncMap[k8sapp.WorkloadRef, activity]

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

	sandboxName string
	threshold   *time.Duration
}

func NewRecorder(ctx context.Context, d dependencies, k8sClient dynamic.Interface) *Recorder {
	r := newRecorder(
		d.Clock(),
		d.Logger().WithComponent("idletimer"),
		newClient(k8sClient, d.Config().K8s.AppsNamespace),
		newRecorderMetrics(d.Telemetry().Meter()),
	)

	d.AppStateWatcher().OnWorkloadRemoved(r.Forget)

	d.Process().OnShutdown(func(ctx context.Context) {
		r.Shutdown(ctx)
	})

	go r.run(ctx)

	return r
}

func newRecorder(clock clockwork.Clock, logger log.Logger, c *client, metrics *recorderMetrics) *Recorder {
	return &Recorder{
		clock:   clock,
		logger:  logger,
		client:  c,
		metrics: metrics,
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

// Observe folds this round's reading of a workload into the Recorder — which
// record it writes to, and how often — and reports the last request this
// replica has seen.
//
// A new member is a new record, so the cadence starts over. Carrying it across
// would leave the new record unwritten while traffic flows, and the other
// replica, reading a record that never advances, would suspend early.
func (r *Recorder) Observe(ref k8sapp.WorkloadRef, sandboxName string, threshold *time.Duration) time.Time {
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
			return
		case <-r.stopping:
			r.sweep(ctx)
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
}

func (r *Recorder) sweep(ctx context.Context) {
	now := r.clock.Now()

	var pending []pendingWrite
	r.workloads.Range(func(ref k8sapp.WorkloadRef, item *activity) {
		item.lock.Lock()
		defer item.lock.Unlock()

		if !writeDue(item, item.lastRequestAt, now) {
			return
		}
		pending = append(pending, pendingWrite{ref: ref, item: item, name: item.sandboxName, at: item.lastRequestAt})
	})

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

	err := r.client.cas(writeCtx, w.name, w.at)

	w.item.lock.Lock()
	defer w.item.lock.Unlock()

	if err != nil {
		w.item.retryAfter = r.clock.Now().Add(retryDelay(w.item, err))
		r.metrics.writeFailures.Add(writeCtx, 1, withReason(k8serrors.ReasonForError(err)))
		r.logger.Warnf(writeCtx, "failed to record activity for workload %q: %s", w.ref, err)
		return
	}

	// A member switch during the write moved this workload to another record.
	// Claiming the cadence now would leave that one unwritten for a heartbeat.
	if w.item.sandboxName != w.name {
		return
	}

	w.item.lastWriteAt = w.at
	w.item.retryAfter = time.Time{}
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
