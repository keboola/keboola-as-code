package apptoken

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
	sdk "github.com/keboola/keboola-sdk-go/v2/pkg/apptoken"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/sessions"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/chain"
)

// HeaderName is the header in which the token is passed on. The header, the claims and the rest of the contract with the services
// that verify the token are in keboola-sdk-go, which they share.
const HeaderName = sdk.HeaderName

// Inject must run after sessions.Middleware, which puts the session into the context.
func (m *Minter) Inject(app api.AppConfig, workload k8sapp.WorkloadRef) chain.Middleware {
	return func(next chain.Handler) chain.Handler {
		return chain.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) error {
			req.Header.Del(HeaderName)
			req.Header.Del(RefreshHeaderName)
			req.Header.Del(PublicURLHeaderName)
			ctx := req.Context()
			id, ok := ctx.Value(identityCtxKey{}).(identity)
			if !ok || !m.keyring.Configured() {
				return next.ServeHTTPOrError(rw, req)
			}

			var sid string
			if s, ok := sessions.FromContext(ctx); ok {
				sid = s.ID
			}
			token, _, err := m.mint(ctx, app, workload, id, sid, time.Time{})
			if err != nil {
				m.logger.Errorf(ctx, "cannot sign the app user token, the request continues without it: %s", err)
				return next.ServeHTTPOrError(rw, req)
			}
			req.Header.Set(HeaderName, token)
			if sessions.IsWebsocketUpgrade(req) {
				m.injectRefresh(req, app, workload, id, sid)
			}
			return next.ServeHTTPOrError(rw, req)
		})
	}
}

// mint signs an app user token. A non-zero notAfter caps its expiry.
func (m *Minter) mint(ctx context.Context, app api.AppConfig, workload k8sapp.WorkloadRef, id identity, sid string, notAfter time.Time) (string, time.Time, error) {
	jti, err := randomID()
	if err != nil {
		return "", time.Time{}, err
	}

	now := m.clock.Now()
	exp := now.Add(m.ttl)
	if !notAfter.IsZero() && notAfter.Before(exp) {
		exp = notAfter
	}
	claims := sdk.Claims{
		Issuer:    m.issuer,
		Audience:  sdk.Audience,
		Subject:   id.subject,
		IDP:       id.idp,
		Email:     id.email,
		Groups:    id.groups,
		Kbc:       sdk.KbcClaims{ProjectID: app.ProjectID, AppID: app.ID.String(), WorkspaceID: m.workspaceID(ctx, workload)},
		ID:        jti,
		SID:       sid,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(exp),
	}

	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = m.keyring.ActiveKid()
	token.Header["typ"] = sdk.TokenType
	signed, err := token.SignedString(m.keyring.ActiveKey())
	if err != nil {
		return "", time.Time{}, err
	}
	return signed, claims.ExpiresAt.Time, nil
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
