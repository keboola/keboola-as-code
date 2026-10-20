package apptoken

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
)

const (
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

// Capture reads the identity headers before sessions.Middleware removes
// X-Kbc-User-Id. It must run before that middleware.
func (m *Minter) Capture() chain.Middleware {
	return func(next chain.Handler) chain.Handler {
		return chain.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) error {
			emailVerified := req.Header.Get(EmailVerifiedHeader)
			req.Header.Del(EmailVerifiedHeader)
			if id, ok := captureIdentity(req, emailVerified); ok {
				req = req.WithContext(context.WithValue(req.Context(), identityCtxKey{}, id))
			}
			return next.ServeHTTPOrError(rw, req)
		})
	}
}

func captureIdentity(req *http.Request, emailVerified string) (identity, bool) {
	idp, ok := identityProviderFromContext(req.Context())
	if !ok {
		return identity{}, false
	}
	subject := req.Header.Get(userIDHeader)
	if subject == "" {
		return identity{}, false
	}

	id := identity{subject: subject, idp: idp.URL}
	if emailVerified == "true" {
		id.email = req.Header.Get(userEmailHeader)
	}
	id.groups = parseGroups(req.Header.Values(userRolesHeader))
	return id, true
}

// parseGroups splits the comma-joined header. A group name containing a comma
// is split too; accepted, the header format cannot carry it.
func parseGroups(values []string) []string {
	var groups []string
	size := 0
	for _, value := range values {
		size += len(value)
		groups = appendNonEmpty(groups, strings.Split(value, ","))
	}
	if size > maxGroupsBytes {
		return nil
	}
	return groups
}

func appendNonEmpty(dst, items []string) []string {
	for _, item := range items {
		if item != "" {
			dst = append(dst, item)
		}
	}
	return dst
}

// Inject must run after sessions.Middleware, which puts the session into the context.
func (m *Minter) Inject(app api.AppConfig, workload k8sapp.WorkloadRef) chain.Middleware {
	return func(next chain.Handler) chain.Handler {
		return chain.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) error {
			req.Header.Del(HeaderName)
			ctx := req.Context()
			id, ok := ctx.Value(identityCtxKey{}).(identity)
			if !ok || !m.keyring.Configured() {
				return next.ServeHTTPOrError(rw, req)
			}

			token, err := m.mint(ctx, app, workload, id)
			if err != nil {
				m.logger.Errorf(ctx, "cannot sign the app user token, the request continues without it: %s", err)
				return next.ServeHTTPOrError(rw, req)
			}
			req.Header.Set(HeaderName, token)
			return next.ServeHTTPOrError(rw, req)
		})
	}
}

func (m *Minter) mint(ctx context.Context, app api.AppConfig, workload k8sapp.WorkloadRef, id identity) (string, error) {
	jti, err := randomID()
	if err != nil {
		return "", err
	}

	now := m.clock.Now()
	claims := Claims{
		Issuer:    m.issuer,
		Audience:  audience,
		Subject:   id.subject,
		IDP:       id.idp,
		Email:     id.email,
		Groups:    id.groups,
		Kbc:       KbcClaims{ProjectID: app.ProjectID, AppID: app.ID.String(), WorkspaceID: m.workspaceID(ctx, workload)},
		ID:        jti,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(m.ttl)),
	}
	if s, ok := sessions.FromContext(ctx); ok {
		claims.SID = s.ID
	}

	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = m.keyring.ActiveKid()
	token.Header["typ"] = tokenType
	return token.SignedString(m.keyring.ActiveKey())
}

// workspaceID is read per request: a promotion moves the App to another
// Sandbox without rebuilding the handler.
func (m *Minter) workspaceID(ctx context.Context, workload k8sapp.WorkloadRef) string {
	info, ok := m.apps.GetState(ctx, workload)
	if !ok {
		return ""
	}
	return info.WorkspaceID
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
