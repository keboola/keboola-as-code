package idletimer

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/dynamic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// wakeWindow is how soon after a suspend a workload running again is read as
// this loop having been wrong rather than as a user coming back.
const wakeWindow = time.Minute

// suspendEnabled gates the suspend action, and nothing else. The tick, the
// record, the CAS, the arithmetic and every metric run either way, so a deploy
// with this off exercises the whole loop against a real apiserver — RBAC,
// create, CAS, ownerReference collection, write volume — without stopping
// anyone's app. What it would have suspended is counted and logged instead, so
// the overlap with the sandboxes-service cron can be compared directly.
//
// Two things it cannot exercise, because nothing is ever suspended:
// wokeSoonAfterSuspend can never fire, and Sleep's live re-check and
// resourceVersion precondition stay untried. Flipping this is their first real
// test, and is its own PR.
const suspendEnabled = false

// writeTimeout bounds an activity write, which runs detached from the request
// that triggered it.
const writeTimeout = 5 * time.Second

// workloadSource is the part of the K8s state watcher this loop needs.
type workloadSource interface {
	HasSynced() bool
	RunningWorkloads(ctx context.Context) k8sapp.WorkloadSnapshot
	Sleep(ctx context.Context, ref k8sapp.WorkloadRef) (bool, error)
}

type Manager struct {
	wg             sync.WaitGroup
	suspendEnabled bool
	clock          clockwork.Clock
	logger         log.Logger
	client         *client
	source         workloadSource
	metrics        *metrics
	stateMap       *syncmap.SyncMap[k8sapp.WorkloadRef, state]
}

// state is this replica's view of one workload. threshold is copied from the
// last tick so the request path can pace its writes without reading the cache.
type state struct {
	lock sync.Mutex

	lastRequestAt time.Time
	lastWriteAt   time.Time

	sandboxName    string
	threshold      time.Duration
	thresholdKnown bool

	suspendedAt time.Time

	// suppressedAt makes a gated suspend edge-triggered. A real suspend takes
	// the workload out of the Running set; a suppressed one leaves it there, so
	// without this every later tick would decide, count and log it again.
	suppressedAt time.Time
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Config() config.Config
	Process() *servicectx.Process
	Telemetry() telemetry.Telemetry
	AppStateWatcher() *k8sapp.StateWatcher
	K8sDynamicClient() dynamic.Interface
}

func NewManager(ctx context.Context, d dependencies) *Manager {
	m := newManager(
		d.Clock(),
		d.Logger().WithComponent("idletimer"),
		newClient(d.K8sDynamicClient(), d.Config().K8s.AppsNamespace),
		d.AppStateWatcher(),
		newMetrics(d.Telemetry().Meter()),
		suspendEnabled,
	)

	d.AppStateWatcher().OnWorkloadRemoved(m.evictWorkload)

	d.Process().OnShutdown(func(ctx context.Context) {
		m.Shutdown(ctx)
	})

	ticker := m.clock.NewTicker(tickInterval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.Chan():
				m.tick(ctx)
			}
		}
	}()

	return m
}

func newManager(clock clockwork.Clock, logger log.Logger, c *client, source workloadSource, metrics *metrics, suspendEnabled bool) *Manager {
	return &Manager{
		suspendEnabled: suspendEnabled,
		clock:          clock,
		logger:         logger,
		client:         c,
		source:         source,
		metrics:        metrics,
		stateMap: syncmap.New[k8sapp.WorkloadRef, state](func(k8sapp.WorkloadRef) *state {
			return &state{}
		}),
	}
}

// Shutdown waits for the activity writes already in flight. They are detached
// from their requests, so nothing else would wait for them.
func (m *Manager) Shutdown(ctx context.Context) {
	m.logger.Info(ctx, "waiting for pending idle timer writes")
	m.wg.Wait()
}

// evictWorkload drops the state for a workload that left the cache, so the map
// does not grow with every workload the process ever saw.
func (m *Manager) evictWorkload(ref k8sapp.WorkloadRef) {
	m.stateMap.Delete(ref)
}

// RecordActivity marks a workload as in use. It is called from the request
// path, so it does its own K8s write only once per heartbeat interval; the
// in-memory mark is what every call updates.
func (m *Manager) RecordActivity(ctx context.Context, ref k8sapp.WorkloadRef) {
	item := m.stateMap.GetOrInit(ref)

	item.lock.Lock()
	now := m.clock.Now()
	item.lastRequestAt = now
	due, name := writeDue(item, now, ref)
	if due {
		item.lastWriteAt = now
	}
	item.lock.Unlock()

	if !due {
		return
	}

	// Detached from the request: this is called from the GotConn callback and,
	// per frame, from the websocket observer, neither of which may wait on the
	// apiserver. context.WithoutCancel keeps the write alive past the response.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		writeCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), writeTimeout, errors.New("idle timer write timeout"))
		defer cancel()

		if err := m.client.cas(writeCtx, name, now); err != nil {
			m.metrics.recordErrors.Add(writeCtx)
			m.logger.Warnf(writeCtx, "failed to record activity for workload %q: %s", ref, err)
		}
	}()
}

// writeDue decides whether this request owes a shared write. Until the first
// tick has reported a threshold the cadence is the shortest one the thresholds
// allow: writing too often only costs writes, while writing too rarely lets
// another replica read a stale value and suspend early.
func writeDue(item *state, now time.Time, ref k8sapp.WorkloadRef) (bool, string) {
	name := item.sandboxName
	if name == "" {
		name = ref.SandboxName
	}
	if name == "" {
		return false, ""
	}
	if item.thresholdKnown && item.threshold == 0 {
		return false, ""
	}
	return now.Sub(item.lastWriteAt) > heartbeatInterval(item.threshold), name
}

func (m *Manager) tick(ctx context.Context) {
	// Absence from a cache that is still filling is not absence of a workload.
	if !m.source.HasSynced() {
		return
	}

	snapshot := m.source.RunningWorkloads(ctx)

	// Counted per round, not accumulated: this gauge is what gates retiring the
	// cron that still suspends apps today, and the question it answers is
	// "is anything being skipped now", not "has anything ever been skipped".
	notSuspendable := int64(snapshot.Unresolved)
	for _, c := range snapshot.Candidates {
		if !m.visit(ctx, c) {
			notSuspendable++
		}
	}
	m.metrics.notSuspendable.Store(notSuspendable)
}

// visit reports whether the workload was considered at all. A workload with no
// threshold never auto-suspends, which is a reason the loop is doing nothing
// and so belongs in the gate.
func (m *Manager) visit(ctx context.Context, c k8sapp.SuspendCandidate) bool {
	item := m.stateMap.GetOrInit(c.Ref)
	now := m.clock.Now()

	item.lock.Lock()
	item.threshold = c.Threshold
	item.thresholdKnown = true
	item.sandboxName = c.SandboxName
	memory := item.lastRequestAt
	suspendedAt := item.suspendedAt
	item.lock.Unlock()

	// A workload that started again shortly after this replica stopped it is
	// the cross-replica error this loop is measured by.
	//
	// The restart is the discriminator, not merely still being in the Running
	// set: a patched workload stays Running until the operator reconciles it
	// and the informer delivers the change, which is this loop working.
	if !suspendedAt.IsZero() {
		if c.LastStarted.After(suspendedAt) && now.Sub(suspendedAt) <= wakeWindow {
			m.metrics.wokeSoonAfterSuspend.Add(ctx)
		}
		if c.LastStarted.After(suspendedAt) || now.Sub(suspendedAt) > wakeWindow {
			m.clearSuspended(item)
		}
	}

	// Absent means never auto-suspend.
	if c.Threshold == 0 {
		return false
	}

	rec, found := m.readRecord(ctx, c)
	if !found {
		return true
	}

	idleFor := now.Sub(lastSeen(memory, rec.lastRequestAt, c.LastStarted))
	if !shouldSuspend(idleFor, c.Threshold) {
		// Back in use: the next time it goes idle is a new episode.
		m.clearSuppressed(item)
		return true
	}

	m.suspend(ctx, c, item, idleFor)
	return true
}

// readRecord returns the workload's shared record, creating it when it is
// genuinely absent. A read that failed for any other reason reports not-found
// so the round is skipped: treating an error as absence would reset every
// record on an apiserver wobble.
func (m *Manager) readRecord(ctx context.Context, c k8sapp.SuspendCandidate) (record, bool) {
	rec, err := m.client.get(ctx, c.SandboxName)
	if err == nil {
		return rec, true
	}

	if !k8serrors.IsNotFound(err) {
		m.metrics.recordErrors.Add(ctx)
		m.logger.Warnf(ctx, "failed to read the idle timer of workload %q: %s", c.Ref, err)
		return record{}, false
	}

	// A record created now gives the workload a full window before it can be
	// judged, which is what makes a never-requested workload suspend at all.
	if err := m.client.create(ctx, c.SandboxName, m.clock.Now()); err != nil {
		m.metrics.recordErrors.Add(ctx)
		m.logger.Warnf(ctx, "failed to create the idle timer of workload %q: %s", c.Ref, err)
	}
	return record{}, false
}

func (m *Manager) suspend(ctx context.Context, c k8sapp.SuspendCandidate, item *state, idleFor time.Duration) {
	if !m.suspendEnabled {
		m.reportSuppressed(ctx, c, item, idleFor)
		return
	}

	suspended, err := m.source.Sleep(ctx, c.Ref)
	if err != nil {
		m.logger.Warnf(ctx, "failed to suspend idle workload %q: %s", c.Ref, err)
		return
	}
	if !suspended {
		return
	}

	m.metrics.suspends.Add(ctx)
	m.logger.Infof(ctx, "suspended idle workload %q", c.Ref)

	item.lock.Lock()
	item.suspendedAt = m.clock.Now()
	item.lock.Unlock()
}

// reportSuppressed records one gated suspend per idle episode, so the overlap
// with the cron can be compared without a line per workload per tick.
func (m *Manager) reportSuppressed(ctx context.Context, c k8sapp.SuspendCandidate, item *state, idleFor time.Duration) {
	item.lock.Lock()
	first := item.suppressedAt.IsZero()
	if first {
		item.suppressedAt = m.clock.Now()
	}
	item.lock.Unlock()

	if !first {
		return
	}

	m.metrics.suspendsSuppressed.Add(ctx)
	m.logger.Infof(ctx, "would suspend workload %q, idle for %s, but the suspend action is off", c.Ref, idleFor)
}

func (m *Manager) clearSuppressed(item *state) {
	item.lock.Lock()
	item.suppressedAt = time.Time{}
	item.lock.Unlock()
}

func (m *Manager) clearSuspended(item *state) {
	item.lock.Lock()
	item.suspendedAt = time.Time{}
	item.lock.Unlock()
}
