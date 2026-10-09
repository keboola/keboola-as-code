package preview

import "strings"

func LandingCSP(nonce string, frameAncestors []string) string {
	return pageCSP(nonce, "'self'", frameAncestors)
}

func SessionRequiredCSP(nonce string, frameAncestors []string) string {
	return pageCSP(nonce, "'none'", frameAncestors)
}

func pageCSP(nonce, formAction string, frameAncestors []string) string {
	ancestors := "'none'"
	if len(frameAncestors) > 0 {
		ancestors = strings.Join(frameAncestors, " ")
	}
	var b strings.Builder
	b.WriteString("default-src 'none'; script-src 'nonce-")
	b.WriteString(nonce)
	b.WriteString("'; form-action ")
	b.WriteString(formAction)
	b.WriteString("; base-uri 'none'; frame-ancestors ")
	b.WriteString(ancestors)
	return b.String()
}
