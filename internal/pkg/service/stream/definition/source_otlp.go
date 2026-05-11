package definition

import "net/url"

const (
	SourceTypeOTLP = SourceType("otlp")

	// OTLP signal names — used in AllowedSignals sink filter.
	OTLPSignalLogs    = "logs"
	OTLPSignalMetrics = "metrics"
	OTLPSignalTraces  = "traces"
)

type OTLPSource struct {
	Secret string `json:"secret" validate:"required,len=48"`
}

// FormatOTLPSourceURL returns the base endpoint URL that an OpenTelemetry SDK
// should be configured with. The SDK appends /v1/logs, /v1/metrics, /v1/traces
// automatically based on the signal being exported.
func (s *Source) FormatOTLPSourceURL(publicURL string) (string, error) {
	u, err := url.Parse(publicURL)
	if err != nil {
		return "", err
	}
	return u.JoinPath("otlp", s.ProjectID.String(), s.SourceID.String(), s.OTLP.Secret).String(), nil
}
