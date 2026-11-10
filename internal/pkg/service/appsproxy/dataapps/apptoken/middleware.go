package apptoken

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"

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

	claims.Attrs = m.attrsFor(ctx, claims, id)

	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = m.keyring.ActiveKid()
	token.Header["typ"] = sdk.TokenType
	return token.SignedString(m.keyring.ActiveKey())
}

// attrsFor returns the attributes of the user for the token: the claims of the allowlist that have a usable value, as many as fit
// the cap of attrs and the budget of the token next to the other claims, in the order of the allowlist. It returns nil when there
// are none, and counts every claim that did not become an attribute.
func (m *Minter) attrsFor(ctx context.Context, claims sdk.Claims, id identity) map[string][]string {
	for _, reason := range []attrDrop{attrDropType, attrDropSize} {
		m.countDropped(ctx, reason, countOf(id.attrDrops, reason))
	}
	if len(id.attrs) == 0 {
		return nil
	}

	header, errHeader := json.Marshal(map[string]string{"alg": sdk.Algorithm, "kid": m.keyring.ActiveKid(), "typ": sdk.TokenType})
	payload, errPayload := json.Marshal(claims) // without attrs: the claims have none yet
	if errHeader != nil || errPayload != nil {
		m.countDropped(ctx, attrDropBudget, len(id.attrs))
		return nil
	}

	attrs, dropped := fitAttrs(id.attrs, len(header), len(payload))
	m.countDropped(ctx, attrDropBudget, dropped)
	return attrs
}

func countOf(drops []attrDrop, reason attrDrop) int {
	n := 0
	for _, drop := range drops {
		if drop == reason {
			n++
		}
	}
	return n
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
