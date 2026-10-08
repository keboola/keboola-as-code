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
// working looks exactly like a fleet that is busy.
type metrics struct {
	// notSuspendable is the current round, not a running total: the decision to
	// retire the old cron needs "nothing was skipped on this pass", and a
	// cumulative value stays non-zero forever after one transient skip.
	notSuspendable atomic.Int64

	suspends             counter
	wokeSoonAfterSuspend counter
	recordErrors         counter
}

// counter keeps a local total beside the emitted one. The total is what tests
// assert on; an OTel counter cannot be read back.
type counter struct {
	total      atomic.Int64
	instrument metric.Int64Counter
}

func (c *counter) Add(ctx context.Context) {
	c.total.Add(1)
	if c.instrument != nil {
		c.instrument.Add(ctx, 1)
	}
}

func (c *counter) Load() int64 {
	return c.total.Load()
}

// newMetrics publishes the instruments. A nil meter keeps the local totals
// without publishing, which is what the unit tests use.
func newMetrics(meter telemetry.Meter) *metrics {
	m := &metrics{}
	if meter == nil {
		return m
	}

	meter.IntObservableGauge(
		"keboola.go.appsproxy.idletimer.not_suspendable",
		"Running workloads this pass could not consider: no autoSuspendAfterSeconds, or an App whose productionSandbox resolved to nothing.",
		"",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(m.notSuspendable.Load())
			return nil
		},
	)

	m.suspends.instrument = meter.IntCounter(
		"keboola.go.appsproxy.idletimer.suspends",
		"Workloads this replica has suspended for inactivity.",
		"",
	)
	m.wokeSoonAfterSuspend.instrument = meter.IntCounter(
		"keboola.go.appsproxy.idletimer.woke_soon_after_suspend",
		"Workloads that started again within a minute of this replica suspending them, which is the cross-replica error rate.",
		"",
	)
	m.recordErrors.instrument = meter.IntCounter(
		"keboola.go.appsproxy.idletimer.record_errors",
		"Failed idle-timer reads and writes, including conflicts that outlived the retry.",
		"",
	)

	return m
}
