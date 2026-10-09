package idletimer

import (
	"context"
	"sync"
	"time"

	"github.com/jonboulle/clockwork"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/dynamic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/syncmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const wakeWindow = time.Minute

const writeTimeout = 5 * time.Second

// Rounds are serial, so a slow one delays the next rather than piling up. This
// stops a wedged call from stopping the loop for good.
const tickTimeout = 2 * tickInterval

// workloadSource is the part of the K8s state watcher this loop needs.
type workloadSource interface {
	HasSynced() bool
	ScanForSuspendCandidates() k8sapp.SuspendScan
	Sleep(ctx context.Context, ref k8sapp.WorkloadRef, expectMember string) (bool, error)
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

	// These are touched only by the tick goroutine.
	warnedUnresolved   map[api.AppID]bool
	loggedRecordErrors map[metav1.StatusReason]bool
	roundRecordErrors  map[metav1.StatusReason]bool
}

type state struct {
	lock sync.Mutex

	lastRequestAt time.Time
	lastWriteAt   time.Time

	sandboxName    string
	threshold      time.Duration
	thresholdKnown bool

	suspendedAt time.Time

	// A real suspend takes the workload out of the Running set and is edge
	// triggered by that. A suppressed one leaves it there, so without this every
	// later tick would count and log it again.
	suppressedAt time.Time
}

type dependencies interface {
	Clock() clockwork.Clock
	Logger() log.Logger
	Config() config.Config
	Process() *servicectx.Process
	Telemetry() telemetry.Telemetry
	AppStateWatcher() *k8sapp.StateWatcher
}

func NewManager(ctx context.Context, d dependencies, k8sClient dynamic.Interface) *Manager {
	m := newManager(
		d.Clock(),
		d.Logger().WithComponent("idletimer"),
		newClient(k8sClient, d.Config().K8s.AppsNamespace),
		d.AppStateWatcher(),
		newMetrics(d.Telemetry().Meter()),
		d.Config().IdleSuspend.Enabled,
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

// Shutdown waits for the activity writes in flight. They are detached from
// their requests, so nothing else would wait for them.
func (m *Manager) Shutdown(ctx context.Context) {
	m.logger.Info(ctx, "waiting for pending idle timer writes")
	m.wg.Wait()
}

func (m *Manager) evictWorkload(ref k8sapp.WorkloadRef) {
	m.stateMap.Delete(ref)
}

// RecordActivity marks a workload as in use. Every call updates the in-memory
// mark; the shared record is written once per heartbeat interval.
func (m *Manager) RecordActivity(ctx context.Context, ref k8sapp.WorkloadRef) {
	item := m.stateMap.GetOrInit(ref)

	item.lock.Lock()
	now := m.clock.Now()
	item.lastRequestAt = now
	due := writeDue(item, now)
	name := item.sandboxName
	previousWriteAt := item.lastWriteAt
	if due {
		// Claimed before the write so concurrent requests do not all fire one.
		item.lastWriteAt = now
	}
	item.lock.Unlock()

	if !due {
		return
	}

	// Detached: this runs in the GotConn callback and, per frame, in the
	// websocket observer, neither of which may wait on the apiserver.
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		writeCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), writeTimeout, errors.New("idle timer write timeout"))
		defer cancel()

		if err := m.client.cas(writeCtx, name, now); err != nil {
			m.metrics.recordErrors.Add(writeCtx, 1)
			m.logger.Warnf(writeCtx, "failed to record activity for workload %q: %s", ref, err)
			m.releaseWriteClaim(item, now, previousWriteAt)
		}
	}()
}

// releaseWriteClaim undoes the claim a failed write made, so the next request
// writes instead of waiting out an interval. Holding the claim would let the
// record fall two heartbeat intervals behind while the margin covers one, and
// the other replica, which never saw this traffic, would suspend early.
func (m *Manager) releaseWriteClaim(item *state, claimed, previous time.Time) {
	item.lock.Lock()
	defer item.lock.Unlock()

	// Only if nothing newer has landed since.
	if item.lastWriteAt.Equal(claimed) {
		item.lastWriteAt = previous
	}
}

// writeDue reports whether this request owes a shared write. The caller holds
// the lock.
//
// A workload the tick has not reached has no record name and writes nothing;
// the tick then creates its record at the moment it first sees it, starting a
// full window. Until a threshold is known the cadence is the shortest one the
// thresholds allow, because writing too rarely lets the other replica read a
// stale value and suspend early.
func writeDue(item *state, now time.Time) bool {
	if item.sandboxName == "" {
		return false
	}
	if item.thresholdKnown && item.threshold == 0 {
		return false
	}
	return now.Sub(item.lastWriteAt) > heartbeatInterval(item.threshold)
}

// warnUnresolved reports each App once rather than once per tick: a standing
// misconfiguration would otherwise never stop logging.
func (m *Manager) warnUnresolved(ctx context.Context, unresolved []api.AppID) {
	current := make(map[api.AppID]bool, len(unresolved))
	for _, appID := range unresolved {
		current[appID] = true
		if !m.warnedUnresolved[appID] {
			m.logger.Warnf(ctx, "App %q is Running but its status.productionSandbox matches no cached Sandbox, skipping idle-suspend", appID)
		}
	}
	m.warnedUnresolved = current
}

func (m *Manager) tick(ctx context.Context) {
	// Absence from a cache that is still filling is not absence of a workload.
	if !m.source.HasSynced() {
		return
	}

	ctx, cancel := context.WithTimeoutCause(ctx, tickTimeout, errors.New("idle suspend tick timeout"))
	defer cancel()

	scan := m.source.ScanForSuspendCandidates()
	m.warnUnresolved(ctx, scan.Unresolved)

	m.roundRecordErrors = map[metav1.StatusReason]bool{}

	notSuspendable := int64(len(scan.Unresolved))
	for _, c := range scan.Candidates {
		// A round that ran out of time would otherwise turn one cancelled
		// context into one failure per remaining workload.
		if ctx.Err() != nil {
			break
		}
		if !m.visit(ctx, c) {
			notSuspendable++
		}
	}
	m.metrics.notSuspendable.Store(notSuspendable)
	m.loggedRecordErrors = m.roundRecordErrors
}

// visit reports whether the workload was considered at all.
func (m *Manager) visit(ctx context.Context, c k8sapp.SuspendCandidate) bool {
	item := m.stateMap.GetOrInit(c.Ref)
	now := m.clock.Now()

	memory, woke := m.observe(item, c, now)

	// The restart is the discriminator, not merely still being Running: a
	// patched workload stays Running until the operator reconciles it.
	if woke {
		m.metrics.wokeSoonAfterSuspend.Add(ctx, 1)
	}

	if c.Threshold == 0 {
		return false
	}

	rec, found := m.readRecord(ctx, c)
	if !found {
		return true
	}

	idleFor := now.Sub(lastSeen(memory, rec.lastRequestAt, c.LastStarted))
	if !shouldSuspend(idleFor, c.Threshold) {
		m.clearSuppressed(item)
		return true
	}

	m.suspend(ctx, c, item, idleFor)
	return true
}

// observe folds this round's reading into the state under one lock, and reports
// the in-memory last request plus whether the workload came back from a suspend
// this replica issued.
func (m *Manager) observe(item *state, c k8sapp.SuspendCandidate, now time.Time) (memory time.Time, woke bool) {
	item.lock.Lock()
	defer item.lock.Unlock()

	item.threshold = c.Threshold
	item.thresholdKnown = true
	item.sandboxName = c.SandboxName

	restarted := c.LastStarted.After(item.suspendedAt)
	if !item.suspendedAt.IsZero() {
		woke = restarted && now.Sub(item.suspendedAt) <= wakeWindow
		if restarted || now.Sub(item.suspendedAt) > wakeWindow {
			item.suspendedAt = time.Time{}
		}
	}

	return item.lastRequestAt, woke
}

// readRecord returns the shared record, creating it only when it is genuinely
// absent. Any other error skips the round: treating an error as absence would
// reset every record on an apiserver wobble.
func (m *Manager) readRecord(ctx context.Context, c k8sapp.SuspendCandidate) (record, bool) {
	rec, err := m.client.get(ctx, c.SandboxName)
	if err == nil {
		return rec, true
	}

	if !k8serrors.IsNotFound(err) {
		m.noteRecordError(ctx, "read", c, err)
		return record{}, false
	}

	// The tick, not only the request path, creates the record: a Running
	// workload nobody has ever requested would otherwise never get one.
	// AlreadyExists means the other replica won the race, which is this outcome.
	if err := m.client.create(ctx, c.SandboxName, c.SandboxUID, m.clock.Now()); err != nil && !k8serrors.IsAlreadyExists(err) {
		m.noteRecordError(ctx, "create", c, err)
	}
	return record{}, false
}

func (m *Manager) suspend(ctx context.Context, c k8sapp.SuspendCandidate, item *state, idleFor time.Duration) {
	if !m.suspendEnabled {
		m.reportSuppressed(ctx, c, item, idleFor)
		return
	}

	suspended, err := m.source.Sleep(ctx, c.Ref, c.SandboxName)
	if err != nil {
		m.logger.Warnf(ctx, "failed to suspend idle workload %q: %s", c.Ref, err)
		return
	}
	if !suspended {
		return
	}

	m.metrics.suspends.Add(ctx, 1)
	m.logger.Infof(ctx, "suspended idle workload %q", c.Ref)

	item.lock.Lock()
	item.suspendedAt = m.clock.Now()
	item.lock.Unlock()
}

// noteRecordError counts every occurrence but logs each kind once. A missing
// RBAC rule or CRD fails for every workload on every tick, so logging per
// workload would bury the line that says what is wrong; record_errors carries
// the continuous signal.
func (m *Manager) noteRecordError(ctx context.Context, verb string, c k8sapp.SuspendCandidate, err error) {
	m.metrics.recordErrors.Add(ctx, 1)

	reason := k8serrors.ReasonForError(err)
	m.roundRecordErrors[reason] = true
	if m.loggedRecordErrors[reason] {
		return
	}
	m.logger.Warnf(ctx, "failed to %s the idle timer of workload %q: %s", verb, c.Ref, err)
}

// reportSuppressed records one gated suspend per idle episode.
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

	m.metrics.suspendsSuppressed.Add(ctx, 1)
	m.logger.Infof(ctx, "would suspend workload %q, idle for %s, but the suspend action is off", c.Ref, idleFor)
}

func (m *Manager) clearSuppressed(item *state) {
	item.lock.Lock()
	item.suppressedAt = time.Time{}
	item.lock.Unlock()
}
