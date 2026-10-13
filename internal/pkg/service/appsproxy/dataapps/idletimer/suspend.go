package idletimer

import (
	"context"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const wakeWindow = time.Minute

// Rounds are serial, so a slow one delays the next rather than piling up. This
// stops a wedged call from stopping the loop for good.
const roundTimeout = 2 * tickInterval

// recordErrorLogInterval bounds how often one kind of record failure is logged.
// A missing RBAC rule or CRD fails for every workload on every round, so logging
// per workload buries the line that says what is wrong. Logging it only once
// would be worse: this log is the only signal that the records are failing, so
// it has to keep saying so until someone fixes it.
const recordErrorLogInterval = time.Minute

// warnUnresolved reports each App once rather than once per round: a standing
// misconfiguration would otherwise never stop logging.
func (m *Suspender) warnUnresolved(ctx context.Context, unresolved []api.AppID) {
	current := make(map[api.AppID]bool, len(unresolved))
	for _, appID := range unresolved {
		current[appID] = true
		if !m.warnedUnresolved[appID] {
			m.logger.Warnf(ctx, "App %q is Running but its status.productionSandbox matches no cached Sandbox, skipping idle-suspend", appID)
		}
	}
	m.warnedUnresolved = current
}

func (m *Suspender) suspendIdleWorkloads(ctx context.Context) {
	// Absence from a cache that is still filling is not absence of a workload.
	if !m.workloads.HasSynced() {
		return
	}

	ctx, cancel := context.WithTimeoutCause(ctx, roundTimeout, errors.New("idle suspend round timeout"))
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
func (m *Suspender) visit(ctx context.Context, c k8sapp.SleepCandidate) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	item := m.states.GetOrInit(c.Ref)
	now := m.clock.Now()

	memory := m.recorder.Track(c.Ref, c.SandboxName, c.Threshold)
	woke := m.observeRestart(item, c, now)

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

	// A cached read lags, and a lagging record reads as idle. With the gate on
	// that would suspend a workload in use; with it off it would report a
	// suspend the enabled loop would not have made, which is the number the
	// rollout is judged on. Confirming once per episode covers the second case
	// without a read per round.
	if rec.cached && (m.suspendEnabled || !m.suppressed(item)) {
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

// observeRestart reports whether the workload came back from a suspend this
// replica issued, and forgets the suspend once it has.
func (m *Suspender) observeRestart(item *suspendState, c k8sapp.SleepCandidate, now time.Time) (woke bool) {
	item.lock.Lock()
	defer item.lock.Unlock()

	restarted := c.LastStarted.After(item.suspendedAt)
	if !item.suspendedAt.IsZero() {
		woke = restarted && now.Sub(item.suspendedAt) <= wakeWindow
		if restarted || now.Sub(item.suspendedAt) > wakeWindow {
			item.suspendedAt = time.Time{}
		}
	}

	return woke
}

// readRecord returns the shared record, creating it only when it is genuinely
// absent. Any other error skips the round: treating an error as absence would
// reset every record on an apiserver wobble.
func (m *Suspender) readRecord(ctx context.Context, c k8sapp.SleepCandidate) (record, bool) {
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

	// The round, not only the request path, creates the record: a Running
	// workload nobody has ever requested would otherwise never get one.
	// AlreadyExists means the other replica won the race, which is this outcome.
	if err := m.client.create(ctx, c.SandboxName, c.SandboxUID, m.clock.Now()); err != nil && !k8serrors.IsAlreadyExists(err) {
		m.noteRecordError(ctx, "create", c, err)
	}
	return record{}, false
}

func (m *Suspender) suspend(ctx context.Context, c k8sapp.SleepCandidate, item *suspendState, idleFor time.Duration) {
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
func (m *Suspender) noteRecordError(ctx context.Context, verb string, c k8sapp.SleepCandidate, err error) {
	reason := k8serrors.ReasonForError(err)
	now := m.clock.Now()
	if last, logged := m.lastRecordErrorLog[reason]; logged && now.Sub(last) < recordErrorLogInterval {
		return
	}
	m.lastRecordErrorLog[reason] = now
	m.logger.Warnf(ctx, "failed to %s the idle timer of workload %q: %s", verb, c.Ref, err)
}

// reportSuppressed records one gated suspend per idle episode.
func (m *Suspender) reportSuppressed(ctx context.Context, c k8sapp.SleepCandidate, item *suspendState, idleFor time.Duration) {
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

func (m *Suspender) suppressed(item *suspendState) bool {
	item.lock.Lock()
	defer item.lock.Unlock()

	return !item.suppressedAt.IsZero()
}

func (m *Suspender) clearSuppressed(item *suspendState) {
	item.lock.Lock()
	item.suppressedAt = time.Time{}
	item.lock.Unlock()
}
