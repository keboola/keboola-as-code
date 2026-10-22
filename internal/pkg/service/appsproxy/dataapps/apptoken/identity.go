package apptoken

import (
	"context"
	"net/http"
	"strings"

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
}

type identityCtxKey struct{}

// WithIdentity wraps the upstream of an OAuth provider, so it runs after
// oauth2-proxy has injected the X-Kbc-User-* headers. Only a request that
// passed through it gets a token.
func WithIdentity(next chain.Handler, idpURL string) chain.Handler {
	return chain.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) error {
		emailVerified := req.Header.Get(EmailVerifiedHeader)
		req.Header.Del(EmailVerifiedHeader)
		if id, ok := captureIdentity(req, idpURL, emailVerified); ok {
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
