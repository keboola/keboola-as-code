package oauthproxy

import (
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Metrics struct {
	providerUnavailable metric.Int64Counter
}

func NewMetrics(meter telemetry.Meter) *Metrics {
	return &Metrics{
		providerUnavailable: meter.IntCounter(
			"keboola.go.appsproxy.auth.provider_unavailable",
			"Requests answered with a 503 page because the identity provider did not answer a session refresh.",
			"",
		),
	}
}
