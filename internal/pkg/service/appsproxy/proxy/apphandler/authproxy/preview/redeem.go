package preview

import (
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
)

const (
	Path                   = config.InternalPrefix + "/preview"
	TokenFormField         = "token"
	MaxRedeemBodySize      = 16 << 10
	InvalidLinkMessage     = "Preview link is invalid or expired. Get a new link."
	CrossSiteRedeemMessage = "The preview link was not submitted from its landing page."
)

// IsSameOriginRedeem does not look at Origin: the landing page sends Referrer-Policy: no-referrer,
// so Chrome posts the form with "Origin: null".
func IsSameOriginRedeem(req *http.Request) bool {
	return req.Header.Get("Sec-Fetch-Site") == "same-origin"
}

func LandingCSP(nonce string, frameAncestors []string) string {
	ancestors := "'none'"
	if len(frameAncestors) > 0 {
		ancestors = strings.Join(frameAncestors, " ")
	}
	var b strings.Builder
	b.WriteString("default-src 'none'; script-src 'nonce-")
	b.WriteString(nonce)
	b.WriteString("'; form-action 'self'; base-uri 'none'; frame-ancestors ")
	b.WriteString(ancestors)
	return b.String()
}
