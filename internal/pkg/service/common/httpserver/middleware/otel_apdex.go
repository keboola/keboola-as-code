package middleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// apdexCounter is a helper to calculate apdex metric and process it by OpenTelemetry.

// Apdex[T] is a value between 0.0-1.0, it is defined by the formula:
// = (satisfied_requests_count + tolerated_requests_count/2) / total_requests_count
//
// Ranges:
// - Satisfied request:  no server error AND duration < T
// - Tolerated request:  no server error AND T < duration < 4T
// - Frustrated request: server error OR duration > 4T
//
// As a metric, it is calculated cumulatively, therefore:
// - Satisfied request  contributes with 1.0 to the total AVG value.
// - Tolerated request  contributes with 0.5 to the total AVG value.
// - Frustrated request contributes with 0.0 to the total AVG value.
type apdexCounter struct {
	satisfiedMs float64
	toleratedMs float64
	counter     metric.Float64UpDownCounter
}

type apdexCounterCollection struct {
	counters []*apdexCounter
}

func apdexCounters(meter metric.Meter, thresholds []time.Duration) *apdexCounterCollection {
	out := &apdexCounterCollection{}
	for _, satisfied := range thresholds {
		name := fmt.Sprintf("keboola_go_http_server_apdex_t%d", satisfied.Milliseconds())
		counter, err := meter.Float64UpDownCounter(name)
		if err != nil {
			panic(err)
		}
		out.counters = append(out.counters, &apdexCounter{
			satisfiedMs: float64(satisfied.Milliseconds()),
			toleratedMs: float64(4 * satisfied.Milliseconds()),
			counter:     counter,
		})
	}
	return out
}

func (c *apdexCounterCollection) Add(ctx context.Context, durationMs float64, statusCode int, opts ...metric.AddOption) {
	for _, c := range c.counters {
		c.Add(ctx, durationMs, statusCode, opts...)
	}
}

func (c *apdexCounter) Add(ctx context.Context, durationMs float64, statusCode int, opts ...metric.AddOption) {
	var value float64
	switch {
	case durationMs <= c.satisfiedMs && statusCode < http.StatusInternalServerError:
		value = 1
	case durationMs <= c.toleratedMs && statusCode < http.StatusInternalServerError:
		value = 0.5
	default:
		value = 0
	}
	c.counter.Add(ctx, value, opts...)
}
