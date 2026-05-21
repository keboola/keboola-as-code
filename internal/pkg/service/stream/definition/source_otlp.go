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

// FormatOTLPSourceURL returns the endpoint URL with the secret embedded as the
// last path segment. Suitable as a single-string convenience value for
// SDK configurations that authenticate by URL only.
func (s *Source) FormatOTLPSourceURL(publicURL string) (string, error) {
	u, err := url.Parse(publicURL)
	if err != nil {
		return "", err
	}
	return u.JoinPath("otlp", s.ProjectID.String(), s.SourceID.String(), s.OTLP.Secret).String(), nil
}

// FormatOTLPSourceBaseURL returns the endpoint URL without the secret. Used
// together with the secret in an Authorization: Bearer header so the secret
// stays out of access/CDN/APM logs.
func (s *Source) FormatOTLPSourceBaseURL(publicURL string) (string, error) {
	u, err := url.Parse(publicURL)
	if err != nil {
		return "", err
	}
	return u.JoinPath("otlp", s.ProjectID.String(), s.SourceID.String()).String(), nil
}
