package idletimer

import (
	"context"
	"sync/atomic"

	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// metrics exist because every way this loop goes wrong is quiet.
//
// Each failure direction ends in "nothing suspends", so a loop that has stopped
// working looks exactly like a fleet that is busy. skippedNoThreshold is the
// one that gates retiring the old cron: it has to read zero on every stack for
// a full idle cycle before the cron can go. recordErrors is the one that can
// break the never-early guarantee, which holds only while the writes land.
type metrics struct {
	skippedNoThreshold   atomic.Int64
	suspends             atomic.Int64
	wokeSoonAfterSuspend atomic.Int64
	recordErrors         atomic.Int64
}

// newMetrics publishes the counters as observable gauges. A nil meter keeps the
// counters without publishing them, which is what the tests use.
func newMetrics(meter telemetry.Meter) *metrics {
	m := &metrics{}
	if meter == nil {
		return m
	}

	observe := func(name, description string, value *atomic.Int64) {
		meter.IntObservableGauge(name, description, "", func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(value.Load())
			return nil
		})
	}

	observe(
		"keboola.go.appsproxy.idletimer.skipped.no_threshold",
		"Running workloads skipped because their Sandbox carries no autoSuspendAfterSeconds.",
		&m.skippedNoThreshold,
	)
	observe(
		"keboola.go.appsproxy.idletimer.suspends",
		"Workloads this replica has suspended for inactivity.",
		&m.suspends,
	)
	observe(
		"keboola.go.appsproxy.idletimer.woke_soon_after_suspend",
		"Suspends followed by the workload running again within a minute, which is the cross-replica error rate.",
		&m.wokeSoonAfterSuspend,
	)
	observe(
		"keboola.go.appsproxy.idletimer.record_errors",
		"Failed idle-timer reads and writes, including conflicts that outlived the retry.",
		&m.recordErrors,
	)

	return m
}
