package idletimer

import (
	"context"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const writeTimeout = 5 * time.Second

// writeRetryInterval holds off a write that failed for a reason a retry may
// clear. Short, because the margin that covers a missed write is one heartbeat.
const writeRetryInterval = time.Second

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
