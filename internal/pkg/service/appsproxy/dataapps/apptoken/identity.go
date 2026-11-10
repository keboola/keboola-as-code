package apptoken

import (
	"context"
	"net/http"
	"strings"

	middlewareapi "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/middleware"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
)

const (
	EmailVerifiedHeader = "X-Kbc-User-Email-Verified"

	userIDHeader    = "X-Kbc-User-Id"
	userEmailHeader = "X-Kbc-User-Email"
	userRolesHeader = "X-Kbc-User-Roles"

	maxGroupsBytes = 4096
)

type identity struct {
	subject string
	idp     string
	email   string
	groups  []string
	// attrs are the claims of the allowlist that have a usable value, in the order of the allowlist. The budget of the token is
	// applied when it is minted. attrDrops are the reasons why the other claims of the allowlist did not become attributes.
	attrs     []attrCandidate
	attrDrops []attrDrop
}

type identityCtxKey struct{}

// WithIdentity wraps the upstream of an OAuth provider, so it runs after
// oauth2-proxy has injected the X-Kbc-User-* headers. Only a request that
// passed through it gets a token. attributeClaims is the allowlist of the
// provider, already validated (nil for none): those claims are read from the
// session of oauth2-proxy, never from a header, so a client cannot send them.
func WithIdentity(next chain.Handler, idpURL string, attributeClaims []string) chain.Handler {
	return chain.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) error {
		emailVerified := req.Header.Get(EmailVerifiedHeader)
		req.Header.Del(EmailVerifiedHeader)
		if id, ok := captureIdentity(req, idpURL, emailVerified); ok {
			id.attrs, id.attrDrops = captureAttrs(req, attributeClaims)
			req = req.WithContext(context.WithValue(req.Context(), identityCtxKey{}, id))
		}
		return next.ServeHTTPOrError(rw, req)
	})
}

func captureIdentity(req *http.Request, idpURL, emailVerified string) (identity, bool) {
	subject := req.Header.Get(userIDHeader)
	if subject == "" {
		return identity{}, false
	}

	id := identity{subject: subject, idp: idpURL}
	if emailVerified == "true" {
		id.email = req.Header.Get(userEmailHeader)
	}
	id.groups = parseGroups(req.Header.Values(userRolesHeader))
	return id, true
}

// captureAttrs reads the allowlisted claims from the login session that oauth2-proxy keeps in the request scope. oauth2-proxy has
// extracted them from the ID token once, at login. A request without a session, or without an allowlist, has no attributes.
func captureAttrs(req *http.Request, allowlist []string) ([]attrCandidate, []attrDrop) {
	if len(allowlist) == 0 {
		return nil, nil
	}
	scope := middlewareapi.GetRequestScope(req)
	if scope == nil || scope.Session == nil {
		return nil, nil
	}

	return mapAttrs(scope.Session.AdditionalClaims, allowlist)
}

// parseGroups splits the comma-joined header. A group name containing a comma
// is split too; accepted, the header format cannot carry it.
func parseGroups(values []string) []string {
	groups := make([]string, 0, len(values))
	size := 0
	for _, value := range values {
		size += len(value)
		groups = append(groups, strings.FieldsFunc(value, func(r rune) bool { return r == ',' })...)
	}
	if size > maxGroupsBytes {
		return nil
	}
	return groups
}
