package idletimer

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	stateCandidate   = "candidate"
	stateNoThreshold = "no_threshold"
	stateUnresolved  = "unresolved"
	stateUnvouched   = "unvouched"
)

const (
	outcomePerformed  = "performed"
	outcomeSuppressed = "suppressed"
)

type metrics struct {
	// Each holds the last round's count for the gauge callback to read. They
	// are gauges, not totals: the question is what the loop sees now. candidate
	// is the denominator the other two are only meaningful against — without it
	// a zero cannot be told from an empty stack.
	candidates  atomic.Int64
	noThreshold atomic.Int64
	unresolved  atomic.Int64
	unvouched   atomic.Int64

	suspends             metric.Int64Counter
	wokeSoonAfterSuspend metric.Int64Counter
}

// recorderMetrics is the Recorder's own instrument. It is built separately
// because registering the gauge's callback twice would observe it twice.
type recorderMetrics struct {
	writeFailures metric.Int64Counter
}

func newRecorderMetrics(meter telemetry.Meter) *recorderMetrics {
	return &recorderMetrics{
		writeFailures: meter.IntCounter(
			"keboola.go.appsproxy.idletimer.write_failures",
			"Activity writes that did not land, by apiserver reason. While they fail this replica's entry goes overdue and it suspends nothing; a replica whose writes still land cannot see this one's traffic.",
			"",
		),
	}
}

func newMetrics(meter telemetry.Meter) *metrics {
	m := &metrics{}

	meter.IntObservableGauge(
		"keboola.go.appsproxy.idletimer.workloads",
		"Running workloads this pass saw, by what the loop can do with them: candidate, no_threshold (no autoSuspendAfterSeconds, so it opted out), unresolved (an App whose productionSandbox matched no cached Sandbox, which is a bug smell), unvouched (the record carries no current entry for this replica: its writes are not landing, or the idle timer CRD has no observers field, so it suspends nothing).",
		"",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(m.candidates.Load(), withState(stateCandidate))
			o.Observe(m.noThreshold.Load(), withState(stateNoThreshold))
			o.Observe(m.unresolved.Load(), withState(stateUnresolved))
			o.Observe(m.unvouched.Load(), withState(stateUnvouched))
			return nil
		},
	)

	m.suspends = meter.IntCounter(
		"keboola.go.appsproxy.idletimer.suspends",
		`Suspend decisions by outcome: performed (the patch landed) or suppressed (the suspend action is off, counted once per idle episode rather than once per round).`,
		"",
	)
	m.wokeSoonAfterSuspend = meter.IntCounter(
		"keboola.go.appsproxy.idletimer.woke_soon_after_suspend",
		"Workloads that started again within a minute of this replica suspending them, which is the cross-replica error rate.",
		"",
	)

	return m
}

func withState(state string) metric.ObserveOption {
	return metric.WithAttributes(attribute.String("state", state))
}

func withOutcome(outcome string) metric.AddOption {
	return metric.WithAttributes(attribute.String("outcome", outcome))
}

func withReason(reason metav1.StatusReason) metric.AddOption {
	return metric.WithAttributes(attribute.String("reason", string(reason)))
}
