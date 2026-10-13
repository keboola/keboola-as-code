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

// writeRetryInterval holds off a write that failed for a reason a retry may
// clear. Short, because the margin that covers a missed write is one heartbeat.
const writeRetryInterval = time.Second

// Rounds are serial, so a slow one delays the next rather than piling up. This
// stops a wedged call from stopping the loop for good.
const tickTimeout = 2 * tickInterval

// recordErrorLogInterval bounds how often one kind of record failure is logged.
// A missing RBAC rule or CRD fails for every workload on every tick, so logging
// per workload buries the line that says what is wrong. Logging it only once
// would be worse: this log is the only signal that the records are failing, so
// it has to keep saying so until someone fixes it.
const recordErrorLogInterval = time.Minute

// k8sWorkloads is the part of the K8s state watcher this loop needs.
type k8sWorkloads interface {
	HasSynced() bool
	ScanForSleepCandidates() k8sapp.SleepScan
	Sleep(ctx context.Context, ref k8sapp.WorkloadRef, expectMember string) (bool, error)
}

type Manager struct {
	// wg counts marks that have been accepted but not yet handled, so a waiter
	// covers the write a mark goes on to make.
	wg sync.WaitGroup
	// A websocket frame can arrive after Shutdown begins: hijacked connections
	// are not drained by the HTTP server. Admission closes under this lock, so
	// no nudge is ever accepted after Wait has started counting down.
	closeLock sync.RWMutex
	closed    bool
	// stopping ends the writers, and is closed only once every accepted nudge
	// has been handled.
	stopping chan struct{}

	suspendEnabled bool
	clock          clockwork.Clock
	logger         log.Logger
	client         *client
	workloads      k8sWorkloads
	records        *recordCache
	metrics        *metrics
	stateMap       *syncmap.SyncMap[k8sapp.WorkloadRef, state]

	// These are touched only by the tick goroutine.
	warnedUnresolved   map[api.AppID]bool
	lastRecordErrorLog map[metav1.StatusReason]time.Time
}

type state struct {
	// nudges wakes this workload's writer. It holds one: a second would tell
	// the writer nothing the first does not.
	nudges chan struct{}
	// stop ends the writer when the workload leaves the cache.
	stop chan struct{}

	lock sync.Mutex

	lastRequestAt time.Time
	// lastWriteAt is the last request time this replica successfully wrote.
	lastWriteAt time.Time
	// retryAfter holds off a write that just failed. A retry a mark later is
	// what keeps a transient failure from costing a heartbeat of margin; one
	// per request is what turned a standing failure into a storm.
	retryAfter time.Time

	sandboxName string
	threshold   *time.Duration

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
		ctx,
		d.Clock(),
		d.Logger().WithComponent("idletimer"),
		newClient(k8sClient, d.Config().K8s.AppsNamespace),
		d.AppStateWatcher(),
		newMetrics(d.Telemetry().Meter()),
		d.Config().IdleSuspend.Enabled,
	)

	m.records = newRecordCache(ctx, k8sClient, d.Config().K8s.AppsNamespace)

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

func newManager(ctx context.Context, clock clockwork.Clock, logger log.Logger, c *client, workloads k8sWorkloads, metrics *metrics, suspendEnabled bool) *Manager {
	m := &Manager{
		stopping:           make(chan struct{}),
		suspendEnabled:     suspendEnabled,
		clock:              clock,
		logger:             logger,
		client:             c,
		workloads:          workloads,
		metrics:            metrics,
		lastRecordErrorLog: map[metav1.StatusReason]time.Time{},
	}

	// GetOrInit runs this once per workload, under the map lock, which is what
	// makes starting the workload's writer here safe.
	m.stateMap = syncmap.New[k8sapp.WorkloadRef, state](func(ref k8sapp.WorkloadRef) *state {
		item := &state{
			nudges: make(chan struct{}, 1),
			stop:   make(chan struct{}),
		}
		go m.writeLoop(ctx, ref, item)
		return item
	})

	return m
}

// Shutdown waits for the activity writes in flight. They are detached from
// their requests, so nothing else would wait for them.
func (m *Manager) Shutdown(ctx context.Context) {
	m.closeLock.Lock()
	m.closed = true
	m.closeLock.Unlock()

	m.logger.Info(ctx, "waiting for pending idle timer writes")
	m.wg.Wait()

	close(m.stopping)
}

func (m *Manager) evictWorkload(ref k8sapp.WorkloadRef) {
	if item, found := m.stateMap.Delete(ref); found {
		close(item.stop)
	}
}

// RecordActivity marks a workload as in use. It runs in the GotConn callback
// and, per frame, in the websocket observer, so it only takes the workload's
// own lock and nudges its writer. Whether a write is owed, and what it carries,
// is the writer's to decide.
//
// The mark is stored here rather than carried on the channel because a nudge
// may be dropped. A dropped nudge is harmless: one is already queued, and the
// writer reads the mark rather than the nudge.
func (m *Manager) RecordActivity(_ context.Context, ref k8sapp.WorkloadRef) {
	item := m.stateMap.GetOrInit(ref)

	item.lock.Lock()
	item.lastRequestAt = m.clock.Now()
	item.lock.Unlock()

	m.closeLock.RLock()
	if m.closed {
		m.closeLock.RUnlock()
		return
	}
	m.wg.Add(1)
	m.closeLock.RUnlock()

	select {
	case item.nudges <- struct{}{}:
	default:
		m.wg.Done()
	}
}

// writeLoop owns the write side of one workload. Serial by construction, so a
// workload never has two writes in flight, while workloads never wait on each
// other.
func (m *Manager) writeLoop(ctx context.Context, ref k8sapp.WorkloadRef, item *state) {
	for {
		select {
		case <-item.nudges:
			m.writeIfDue(ctx, ref, item)
			m.wg.Done()
		case <-item.stop:
			// Eviction is the only exit that can strand nudges: Shutdown closes
			// stopping only once every one of them has been handled.
			m.drainNudges(item)
			return
		case <-m.stopping:
			return
		}
	}
}

// drainNudges accounts for the nudges a stopped writer will not act on, so a
// waiter does not hang on them.
func (m *Manager) drainNudges(item *state) {
	for {
		select {
		case <-item.nudges:
			m.wg.Done()
		default:
			return
		}
	}
}

func (m *Manager) writeIfDue(ctx context.Context, ref k8sapp.WorkloadRef, item *state) {
	item.lock.Lock()
	at := item.lastRequestAt
	name := item.sandboxName
	due := writeDue(item, at, m.clock.Now())
	item.lock.Unlock()

	if !due {
		return
	}

	writeCtx, cancel := context.WithTimeoutCause(context.WithoutCancel(ctx), writeTimeout, errors.New("idle timer write timeout"))
	defer cancel()

	err := m.client.cas(writeCtx, name, at)

	item.lock.Lock()
	defer item.lock.Unlock()

	if err == nil {
		item.lastWriteAt = at
		item.retryAfter = time.Time{}
		return
	}

	item.retryAfter = m.clock.Now().Add(retryDelay(item, err))
	m.logger.Warnf(writeCtx, "failed to record activity for workload %q: %s", ref, err)
}

// retryDelay holds a failed write off for one interval, and for a whole
// heartbeat when retrying cannot fix it: an absent record is created by the
// tick, and a denied one needs an RBAC change.
func retryDelay(item *state, err error) time.Duration {
	if (k8serrors.IsNotFound(err) || k8serrors.IsForbidden(err)) && item.threshold != nil {
		return heartbeatInterval(*item.threshold)
	}
	return writeRetryInterval
}

// writeDue reports whether this workload owes a shared write. The caller holds
// the lock.
//
// A workload the tick has not reached has no record name and writes nothing;
// the tick then creates its record at the moment it first sees it, starting a
// full window. The name and the threshold are set together, so a known name
// means the threshold is known too.
func writeDue(item *state, at, now time.Time) bool {
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
	if !m.workloads.HasSynced() {
		return
	}

	ctx, cancel := context.WithTimeoutCause(ctx, tickTimeout, errors.New("idle suspend tick timeout"))
	defer cancel()

	scan := m.workloads.ScanForSleepCandidates()
	m.warnUnresolved(ctx, scan.Unresolved)

	var candidates, noThreshold int64
	for _, c := range scan.Candidates {
		state, err := m.visit(ctx, c)
		if err != nil {
			break
		}
		if state == stateCandidate {
			candidates++
		} else {
			noThreshold++
		}
	}
	m.metrics.candidates.Store(candidates)
	m.metrics.noThreshold.Store(noThreshold)
	m.metrics.unresolved.Store(int64(len(scan.Unresolved)))
}

// visit reports the state to count this workload under, and an error when the
// round is over: a round that ran out of time would otherwise turn one
// cancelled context into one failure per remaining workload.
func (m *Manager) visit(ctx context.Context, c k8sapp.SleepCandidate) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	item := m.stateMap.GetOrInit(c.Ref)
	now := m.clock.Now()

	memory, woke := m.observe(item, c, now)

	// The restart is the discriminator, not merely still being Running: a
	// patched workload stays Running until the operator reconciles it.
	if woke {
		m.metrics.wokeSoonAfterSuspend.Add(ctx, 1)
	}

	if c.Threshold == nil {
		return stateNoThreshold, nil
	}

	rec, found := m.readRecord(ctx, c)
	if !found {
		return stateCandidate, nil
	}

	idleFor := now.Sub(lastSeen(memory, rec.lastRequestAt, c.LastStarted))
	if !shouldSuspend(idleFor, *c.Threshold) {
		m.clearSuppressed(item)
		return stateCandidate, nil
	}

	// Only the action needs the confirmation, and the gate is what decides
	// whether there is one.
	if rec.cached && m.suspendEnabled {
		live, err := m.client.get(ctx, c.SandboxName)
		if err != nil {
			m.noteRecordError(ctx, "confirm", c, err)
			return stateCandidate, nil
		}
		idleFor = now.Sub(lastSeen(memory, live.lastRequestAt, c.LastStarted))
		if !shouldSuspend(idleFor, *c.Threshold) {
			m.clearSuppressed(item)
			return stateCandidate, nil
		}
	}

	m.suspend(ctx, c, item, idleFor)
	return stateCandidate, nil
}

// observe folds this round's reading into the state under one lock, and reports
// the in-memory last request plus whether the workload came back from a suspend
// this replica issued.
func (m *Manager) observe(item *state, c k8sapp.SleepCandidate, now time.Time) (memory time.Time, woke bool) {
	item.lock.Lock()
	defer item.lock.Unlock()

	item.threshold = c.Threshold
	if item.sandboxName != c.SandboxName {
		// The cadence belongs to the record, and a new member is a new record.
		// Carrying it over leaves the new one unwritten while traffic flows.
		item.sandboxName = c.SandboxName
		item.lastWriteAt = time.Time{}
		item.retryAfter = time.Time{}
	}

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
func (m *Manager) readRecord(ctx context.Context, c k8sapp.SleepCandidate) (record, bool) {
	if rec, found := m.records.lookup(c.SandboxName); found {
		return rec, true
	}

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

func (m *Manager) suspend(ctx context.Context, c k8sapp.SleepCandidate, item *state, idleFor time.Duration) {
	if !m.suspendEnabled {
		m.reportSuppressed(ctx, c, item, idleFor)
		return
	}

	suspended, err := m.workloads.Sleep(ctx, c.Ref, c.SandboxName)
	if err != nil {
		m.logger.Warnf(ctx, "failed to suspend idle workload %q: %s", c.Ref, err)
		return
	}
	if !suspended {
		m.logger.Debugf(ctx, "idle workload %q was not suspended: it changed since the decision", c.Ref)
		return
	}

	m.metrics.suspends.Add(ctx, 1, withOutcome(outcomePerformed))
	m.logger.Infof(ctx, "suspended idle workload %q", c.Ref)

	item.lock.Lock()
	item.suspendedAt = m.clock.Now()
	item.lock.Unlock()
}

// noteRecordError logs each kind of failure at most once per
// recordErrorLogInterval, however many workloads hit it.
func (m *Manager) noteRecordError(ctx context.Context, verb string, c k8sapp.SleepCandidate, err error) {
	reason := k8serrors.ReasonForError(err)
	now := m.clock.Now()
	if last, logged := m.lastRecordErrorLog[reason]; logged && now.Sub(last) < recordErrorLogInterval {
		return
	}
	m.lastRecordErrorLog[reason] = now
	m.logger.Warnf(ctx, "failed to %s the idle timer of workload %q: %s", verb, c.Ref, err)
}

// reportSuppressed records one gated suspend per idle episode.
func (m *Manager) reportSuppressed(ctx context.Context, c k8sapp.SleepCandidate, item *state, idleFor time.Duration) {
	item.lock.Lock()
	first := item.suppressedAt.IsZero()
	if first {
		item.suppressedAt = m.clock.Now()
	}
	item.lock.Unlock()

	if !first {
		return
	}

	m.metrics.suspends.Add(ctx, 1, withOutcome(outcomeSuppressed))
	m.logger.Infof(ctx, "would suspend workload %q, idle for %s, but the suspend action is off", c.Ref, idleFor)
}

func (m *Manager) clearSuppressed(item *state) {
	item.lock.Lock()
	item.suppressedAt = time.Time{}
	item.lock.Unlock()
}
