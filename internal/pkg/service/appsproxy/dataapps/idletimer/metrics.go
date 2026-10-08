package idletimer

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type metrics struct {
	// notSuspendable holds the last round's count for the gauge callback to
	// read. It is a gauge, not a total: the question it answers is whether
	// anything is being skipped now.
	notSuspendable atomic.Int64

	suspends             metric.Int64Counter
	suspendsSuppressed   metric.Int64Counter
	wokeSoonAfterSuspend metric.Int64Counter
	recordErrors         metric.Int64Counter
}

func newMetrics(meter telemetry.Meter) *metrics {
	m := &metrics{}

	meter.IntObservableGauge(
		"keboola.go.appsproxy.idletimer.not_suspendable",
		"Running workloads this pass could not consider: no autoSuspendAfterSeconds, or an App whose productionSandbox resolved to nothing.",
		"",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(m.notSuspendable.Load())
			return nil
		},
	)

	m.suspends = meter.IntCounter(
		"keboola.go.appsproxy.idletimer.suspends",
		"Workloads this replica has suspended for inactivity.",
		"",
	)
	m.suspendsSuppressed = meter.IntCounter(
		"keboola.go.appsproxy.idletimer.suspends_suppressed",
		"Workloads this replica would have suspended, counted once per idle episode, while the suspend action is off.",
		"",
	)
	m.wokeSoonAfterSuspend = meter.IntCounter(
		"keboola.go.appsproxy.idletimer.woke_soon_after_suspend",
		"Workloads that started again within a minute of this replica suspending them, which is the cross-replica error rate.",
		"",
	)
	m.recordErrors = meter.IntCounter(
		"keboola.go.appsproxy.idletimer.record_errors",
		"Failed idle-timer reads and writes, including conflicts that outlived the retry.",
		"",
	)

	return m
}
