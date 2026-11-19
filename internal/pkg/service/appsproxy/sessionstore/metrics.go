package sessionstore

import (
	"context"
	"net"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Metrics struct {
	duration    metric.Float64Histogram
	errors      metric.Int64Counter
	unavailable metric.Int64Counter
}

// NewMetrics registers the session store metrics and adds a hook to the client that records each command.
func NewMetrics(meter telemetry.Meter, client *redis.Client, checker *Checker) *Metrics {
	m := &Metrics{
		duration: meter.FloatHistogram(
			"keboola.go.appsproxy.sessionstore.duration",
			"Duration of a login session store command.",
			"ms",
		),
		errors: meter.IntCounter(
			"keboola.go.appsproxy.sessionstore.errors",
			"Failed login session store commands. A missing key is not an error.",
			"",
		),
		unavailable: meter.IntCounter(
			"keboola.go.appsproxy.sessionstore.unavailable",
			"Requests answered with 503 because the login session store was unavailable.",
			"",
		),
	}

	meter.IntObservableGauge(
		"keboola.go.appsproxy.sessionstore.up",
		"1 if the last ping of the login session store succeeded, 0 otherwise.",
		"",
		func(_ context.Context, o metric.Int64Observer) error {
			if checker.Up() {
				o.Observe(1)
			} else {
				o.Observe(0)
			}
			return nil
		},
	)
	meter.IntObservableGauge(
		"keboola.go.appsproxy.sessionstore.pool.connections",
		"Connections in the login session store client pool.",
		"",
		func(_ context.Context, o metric.Int64Observer) error {
			stats := client.PoolStats()
			o.Observe(int64(stats.TotalConns), metric.WithAttributes(attribute.String("state", "total")))
			o.Observe(int64(stats.IdleConns), metric.WithAttributes(attribute.String("state", "idle")))
			return nil
		},
	)
	meter.IntObservableGauge(
		"keboola.go.appsproxy.sessionstore.pool.timeouts",
		"Times the login session store client waited for a free connection and timed out, since start.",
		"",
		func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(client.PoolStats().Timeouts))
			return nil
		},
	)

	client.AddHook(&hook{metrics: m})
	return m
}

// StoreUnavailable counts a 503 answered because the store was unavailable. A nil receiver does nothing.
func (m *Metrics) StoreUnavailable(ctx context.Context) {
	if m == nil {
		return
	}
	m.unavailable.Add(ctx, 1)
}

func (m *Metrics) record(ctx context.Context, name string, start time.Time, err error) {
	command := commandName(name)
	result := "ok"
	switch {
	case errors.Is(err, redis.Nil):
		result = "miss"
	case err != nil:
		result = "error"
		m.errors.Add(ctx, 1, metric.WithAttributes(attribute.String("command", command)))
	}
	m.duration.Record(ctx, float64(time.Since(start))/float64(time.Millisecond), metric.WithAttributes(
		attribute.String("command", command),
		attribute.String("result", result),
	))
}

func commandName(name string) string {
	switch name {
	case "get", "set", "del":
		return name
	case "eval", "evalsha":
		return "lock"
	default:
		return "other"
	}
}

type hook struct {
	metrics *Metrics
}

func (h *hook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *hook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmd)
		h.metrics.record(ctx, cmd.Name(), start, err)
		return err
	}
}

func (h *hook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			h.metrics.record(ctx, cmd.Name(), start, cmd.Err())
		}
		return err
	}
}
