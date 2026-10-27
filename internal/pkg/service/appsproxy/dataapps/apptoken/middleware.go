package apptoken

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"github.com/golang-jwt/jwt/v5"
	sdk "github.com/keboola/keboola-sdk-go/v2/pkg/apptoken"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
)

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
	claims := sdk.Claims{
		Issuer:    m.issuer,
		Audience:  sdk.Audience,
		Subject:   id.subject,
		IDP:       id.idp,
		Email:     id.email,
		Groups:    id.groups,
		Kbc:       sdk.KbcClaims{ProjectID: app.ProjectID, AppID: app.ID.String(), WorkspaceID: m.workspaceID(ctx, workload)},
		ID:        jti,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(now.Add(m.ttl)),
	}
	if s, ok := sessions.FromContext(ctx); ok {
		claims.SID = s.ID
	}

	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = m.keyring.ActiveKid()
	token.Header["typ"] = sdk.TokenType
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
