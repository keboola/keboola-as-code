package middleware

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/urfave/negroni/v3"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
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
	counter    metric.Int64Counter
	thresholds []*apdexThreshold
}

type apdexThreshold struct {
	sum         metric.Float64Counter
	satisfiedMs float64
	toleratedMs float64
}

func OpenTelemetryApdex(mp metric.MeterProvider) Middleware {
	meter := mp.Meter("otel-middleware")
	apdex := newApdexCounter(meter, []time.Duration{
		500 * time.Millisecond,
		1000 * time.Millisecond,
		2000 * time.Millisecond,
	})

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if isTelemetryDisabled(req) {
				next.ServeHTTP(w, req)
				return
			}

			// Process request
			startTime := time.Now()
			rw := negroni.NewResponseWriter(w)
			next.ServeHTTP(rw, req)

			// Record apdex metric
			ctx := req.Context()
			labeler, _ := otelhttp.LabelerFromContext(ctx)
			elapsedTime := float64(time.Since(startTime)) / float64(time.Millisecond)
			apdex.Add(ctx, req.Method, rw.Status(), elapsedTime, metric.WithAttributes(labeler.Get()...))
		})
	}
}

func newApdexCounter(meter metric.Meter, thresholds []time.Duration) *apdexCounter {
	out := &apdexCounter{counter: telemetry.MustInstrument(meter.Int64Counter("keboola_go_http_server_apdex_count"))}
	for _, satisfied := range thresholds {
		out.thresholds = append(out.thresholds, &apdexThreshold{
			sum:         telemetry.MustInstrument(meter.Float64Counter(fmt.Sprintf("keboola_go_http_server_apdex_%d_sum", satisfied.Milliseconds()))),
			satisfiedMs: float64(satisfied.Milliseconds()),
			toleratedMs: float64(4 * satisfied.Milliseconds()),
		})
	}
	return out
}

func (c *apdexCounter) Add(ctx context.Context, method string, statusCode int, durationMs float64, opts ...metric.AddOption) {
	// Ignore fast/no-operation options method
	if method == http.MethodOptions {
		return
	}

	c.counter.Add(ctx, 1, opts...)
	for _, t := range c.thresholds {
		var value float64
		switch {
		case durationMs <= t.satisfiedMs && statusCode < http.StatusInternalServerError:
			value = 1
		case durationMs <= t.toleratedMs && statusCode < http.StatusInternalServerError:
			value = 0.5
		default:
			value = 0
		}
		t.sum.Add(ctx, value, opts...)
	}
}
