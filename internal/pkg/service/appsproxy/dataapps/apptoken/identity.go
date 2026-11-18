package apptoken

import (
	"context"
	"net/http"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
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
	providerID string
	subject    string
	idp        string
	email      string
	groups     []string
	// allGroups is not limited in size: it decides the matched groups of a refresh token, never goes into a token.
	allGroups []string
}

type identityCtxKey struct{}

// WithIdentity wraps the upstream of an OAuth provider, so it runs after
// oauth2-proxy has injected the X-Kbc-User-* headers. Only a request that
// passed through it gets a token.
func WithIdentity(next chain.Handler, providerID provider.ID, idpURL string) chain.Handler {
	return chain.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) error {
		emailVerified := req.Header.Get(EmailVerifiedHeader)
		req.Header.Del(EmailVerifiedHeader)
		if id, ok := captureIdentity(req, providerID, idpURL, emailVerified); ok {
			req = req.WithContext(context.WithValue(req.Context(), identityCtxKey{}, id))
		}
		return next.ServeHTTPOrError(rw, req)
	})
}

func captureIdentity(req *http.Request, providerID provider.ID, idpURL, emailVerified string) (identity, bool) {
	subject := req.Header.Get(userIDHeader)
	if subject == "" {
		return identity{}, false
	}

	id := identity{providerID: providerID.String(), subject: subject, idp: idpURL}
	if emailVerified == "true" {
		id.email = req.Header.Get(userEmailHeader)
	}
	id.groups = parseGroups(req.Header.Values(userRolesHeader))
	id.allGroups = splitGroups(req.Header.Values(userRolesHeader))
	return id, true
}

// parseGroups is splitGroups limited to maxGroupsBytes of header.
func parseGroups(values []string) []string {
	size := 0
	for _, value := range values {
		size += len(value)
	}
	if size > maxGroupsBytes {
		return nil
	}
	return splitGroups(values)
}

type loginSessionCtxKey struct{}

// WithLoginSession records the session store key of the request's login session; a refresh token carries it.
func WithLoginSession(ctx context.Context, key string) context.Context {
	return context.WithValue(ctx, loginSessionCtxKey{}, key)
}

func loginSessionFrom(ctx context.Context) (string, bool) {
	key, ok := ctx.Value(loginSessionCtxKey{}).(string)
	return key, ok && key != ""
}

// splitGroups splits the comma-joined header. A group name containing a comma
// is split too; accepted, the header format cannot carry it.
func splitGroups(values []string) []string {
	groups := make([]string, 0, len(values))
	for _, value := range values {
		groups = append(groups, strings.FieldsFunc(value, func(r rune) bool { return r == ',' })...)
	}
	return groups
}
