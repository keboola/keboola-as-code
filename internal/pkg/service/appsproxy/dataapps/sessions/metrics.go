package sessions

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

// metrics exist because every way this package fails is silent.
//
// A full queue drops events and a failed send is not retried — deliberately,
// since a retried heartbeat would land as a second row carrying the same delta
// — so nothing surfaces except a log line, and only the first and every
// thousandth at that. A log line cannot be alerted on and cannot be graphed,
// which leaves no way to notice events being lost, or the in-memory store
// growing without bound.
type metrics struct {
	sent    metric.Int64Counter
	dropped metric.Int64Counter
}

func newMetrics(meter telemetry.Meter, tracked func() int) *metrics {
	// Observed on collection rather than counted on every change: the store is
	// a gauge by nature, and reading its length is cheap.
	meter.IntObservableGauge(
		"keboola.go.appsproxy.sessions.tracked",
		"Data app sessions held in memory by this replica.",
		"",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(tracked()))
			return nil
		},
	)

	return &metrics{
		sent: meter.IntCounter(
			"keboola.go.appsproxy.sessions.events.sent",
			"Data app session events handed to Stream, by outcome.",
			"",
		),
		dropped: meter.IntCounter(
			"keboola.go.appsproxy.sessions.events.dropped",
			"Data app session events dropped without being sent, because the queue was full.",
			"",
		),
	}
}

// sentAttrs splits the sent counter by outcome, so the error rate is visible
// without a second metric. The error type is included because the two cases
// mean different things: a timeout is Stream being slow, a 4xx is usually the
// sink mapping or the url being wrong.
func sentAttrs(err error) metric.MeasurementOption {
	if err == nil {
		return metric.WithAttributes(attribute.Bool("is_success", true))
	}
	return metric.WithAttributes(
		attribute.Bool("is_success", false),
		attribute.String("error_type", telemetry.ErrorType(err)),
	)
}
