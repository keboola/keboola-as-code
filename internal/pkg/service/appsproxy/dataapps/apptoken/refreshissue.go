package apptoken

import (
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/upstream"
)

type refreshSession struct {
	key           string
	matchedGroups []string
}

// RefreshEnabled reports whether refresh tokens are issued. Their keys are derived from the signing keys.
func (m *Minter) RefreshEnabled() bool {
	return m.keyring.Configured()
}

// injectRefresh adds the refresh token and the public URL the app sends it to. The refresh token expires when
// apps-proxy closes the websocket, so the reconnect authenticates again.
func (m *Minter) injectRefresh(req *http.Request, app api.AppConfig, workload k8sapp.WorkloadRef, id identity, sid string) {
	if !m.RefreshEnabled() {
		return
	}
	ctx := req.Context()
	loginSession, ok := loginSessionFrom(ctx)
	if !ok {
		return
	}
	info, known := m.apps.GetState(workload)
	origin, ok := k8sapp.PublicOrigin(m.publicURL, app, workload, info, known)
	if !ok {
		m.logger.Warnf(ctx, `no public URL known for "%s", the websocket gets no app user refresh token`, workload.String())
		return
	}

	now := m.clock.Now()
	exp, _ := upstream.WebsocketDeadline(ctx, now, m.wsTimeout)
	matched, err := handshakeMatchedGroups(app, id, sessionGroupsFrom(ctx))
	if err != nil {
		m.logger.Errorf(ctx, "cannot read the allowed groups, the websocket gets no app user refresh token: %s", err)
		return
	}
	token, err := m.signRefresh(app, workload, id, sid, refreshSession{key: loginSession, matchedGroups: matched}, now, exp)
	if err != nil {
		m.logger.Errorf(ctx, "cannot sign the app user refresh token, the websocket continues without it: %s", err)
		return
	}
	req.Header.Set(RefreshHeaderName, token)
	req.Header.Set(PublicURLHeaderName, origin)
}

func (m *Minter) signRefresh(app api.AppConfig, workload k8sapp.WorkloadRef, id identity, sid string, session refreshSession, now, exp time.Time) (string, error) {
	jti, err := randomID()
	if err != nil {
		return "", err
	}
	claims := refreshClaims{
		Issuer:   m.issuer,
		Audience: refreshAudience,
		Subject:  id.subject,
		IDP:      id.idp,
		Email:    id.email,
		Groups:   id.groups,
		Kbc: refreshKbcClaims{
			ProjectID:      app.ProjectID,
			AppID:          app.ID.String(),
			Sandbox:        workload.SandboxName,
			AuthProviderID: id.providerID,
			LoginSession:   session.key,
			MatchedGroups:  session.matchedGroups,
		},
		ID:        jti,
		SID:       sid,
		IssuedAt:  jwt.NewNumericDate(now),
		ExpiresAt: jwt.NewNumericDate(exp),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token.Header["kid"] = m.keyring.ActiveKid()
	token.Header["typ"] = refreshTokenType
	return token.SignedString(m.keyring.activeRefreshKey())
}

func handshakeMatchedGroups(app api.AppConfig, id identity, sessionGroups []string) ([]string, error) {
	oauth, ok := oauthProvider(app, provider.ID(id.providerID))
	if !ok {
		return nil, nil
	}
	allowed, err := allowedGroups(oauth)
	if err != nil {
		return nil, err
	}
	return matchingGroups(sessionGroups, allowed), nil
}
