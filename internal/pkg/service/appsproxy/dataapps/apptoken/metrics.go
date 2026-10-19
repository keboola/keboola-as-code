package apptoken

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

const (
	skipDisabled   = "disabled"
	skipNoIdentity = "no_identity"

	omitGroupsUnknown  = "groups_unknown"
	omitGroupsTooLarge = "groups_too_large"
	omitWorkspace      = "workspace_unknown"
	omitEmailUntrusted = "email_untrusted"
)

type metrics struct {
	minted       metric.Int64Counter
	skipped      metric.Int64Counter
	claimOmitted metric.Int64Counter
}

func newMetrics(meter telemetry.Meter) *metrics {
	return &metrics{
		minted: meter.IntCounter(
			"keboola.go.appsproxy.app_user_token.minted",
			"App user tokens injected into upstream requests.",
			"",
		),
		skipped: meter.IntCounter(
			"keboola.go.appsproxy.app_user_token.skipped",
			"Upstream requests that got no app user token, by reason.",
			"",
		),
		claimOmitted: meter.IntCounter(
			"keboola.go.appsproxy.app_user_token.claim_omitted",
			"Claims left out of a minted app user token, by reason.",
			"",
		),
	}
}

func (m *metrics) skip(ctx context.Context, reason string) {
	m.skipped.Add(ctx, 1, withReason(reason))
}

func (m *metrics) omit(ctx context.Context, reason string) {
	m.claimOmitted.Add(ctx, 1, withReason(reason))
}

func withReason(reason string) metric.AddOption {
	return metric.WithAttributes(attribute.String("reason", reason))
}
