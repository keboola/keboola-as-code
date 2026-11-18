package apptoken

import (
	"context"
	"net/http"
	"slices"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/proxy/apphandler/upstream"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

const (
	RefreshHeaderName   = "X-Kbc-App-User-Refresh-Token" //nolint:gosec // G101: a header name, not a credential
	PublicURLHeaderName = "X-Kbc-App-Public-Url"
	RefreshPath         = config.InternalPrefix + "/app-user-token"

	refreshTokenType = "kbc-app-user-refresh+jwt" //nolint:gosec // G101: the JOSE typ value, not a credential
	refreshAudience  = "apps-proxy"
)

// missingKeyTTL is what PTTL answers for a key that does not exist.
const missingKeyTTL = -2 * time.Nanosecond

// ErrSessionStoreUnavailable means the login session of a refresh token could not be checked.
var ErrSessionStoreUnavailable = errors.New("login session store unavailable")

type refreshClaims struct {
	Issuer    string           `json:"iss"`
	Audience  string           `json:"aud"`
	Subject   string           `json:"sub"`
	IDP       string           `json:"idp"`
	Email     string           `json:"email,omitempty"`
	Groups    []string         `json:"groups,omitempty"`
	Kbc       refreshKbcClaims `json:"kbc"`
	ID        string           `json:"jti"`
	SID       string           `json:"sid,omitempty"`
	IssuedAt  *jwt.NumericDate `json:"iat"`
	ExpiresAt *jwt.NumericDate `json:"exp"`
}

type refreshKbcClaims struct {
	ProjectID      string   `json:"project_id"` //nolint:tagliatelle // the same names as the kbc claim of the app user token
	AppID          string   `json:"app_id"`     //nolint:tagliatelle // the same names as the kbc claim of the app user token
	Sandbox        string   `json:"sandbox,omitempty"`
	AuthProviderID string   `json:"auth_provider_id"`         //nolint:tagliatelle // the same names as the kbc claim of the app user token
	LoginSession   string   `json:"login_session"`            //nolint:tagliatelle // snake case like the other kbc claims
	MatchedGroups  []string `json:"matched_groups,omitempty"` //nolint:tagliatelle // snake case like the other kbc claims
}

func (c refreshClaims) GetExpirationTime() (*jwt.NumericDate, error) { return c.ExpiresAt, nil }

func (c refreshClaims) GetIssuedAt() (*jwt.NumericDate, error) { return c.IssuedAt, nil }

func (c refreshClaims) GetNotBefore() (*jwt.NumericDate, error) { return nil, nil } //nolint:nilnil // there is no nbf claim

func (c refreshClaims) GetIssuer() (string, error) { return c.Issuer, nil }

func (c refreshClaims) GetSubject() (string, error) { return c.Subject, nil }

func (c refreshClaims) GetAudience() (jwt.ClaimStrings, error) {
	return jwt.ClaimStrings{c.Audience}, nil
}

func (c refreshClaims) identity() identity {
	return identity{providerID: c.Kbc.AuthProviderID, subject: c.Subject, idp: c.IDP, email: c.Email, groups: c.Groups}
}

// RefreshEnabled reports whether refresh tokens are issued. Their keys are derived from the signing keys.
func (m *Minter) RefreshEnabled() bool {
	return m.keyring.Configured() && m.store != nil
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
	matched, err := handshakeMatchedGroups(app, id)
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

type refreshSession struct {
	key           string
	matchedGroups []string
}

func handshakeMatchedGroups(app api.AppConfig, id identity) ([]string, error) {
	oauth, ok := oauthProvider(app, provider.ID(id.providerID))
	if !ok {
		return nil, nil
	}
	allowed, err := allowedGroups(oauth)
	if err != nil {
		return nil, err
	}
	return matchingGroups(id.allGroups, allowed), nil
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

// Refresh mints a new app user token from a refresh token issued for this workload.
// The error never contains the token.
func (m *Minter) Refresh(ctx context.Context, app api.AppConfig, workload k8sapp.WorkloadRef, refreshToken string) (string, time.Time, error) {
	if !m.RefreshEnabled() {
		return "", time.Time{}, errors.New("app user token refresh is disabled")
	}
	claims, err := m.verifyRefresh(refreshToken)
	if err != nil {
		return "", time.Time{}, err
	}
	if err := checkWorkload(claims, app, workload); err != nil {
		return "", time.Time{}, err
	}
	if err := checkProvider(claims, app); err != nil {
		return "", time.Time{}, err
	}
	if err := m.checkLoginSession(ctx, claims.Kbc.LoginSession); err != nil {
		return "", time.Time{}, err
	}
	return m.mint(app, workload, claims.identity(), claims.SID, claims.ExpiresAt.Time)
}

func (m *Minter) verifyRefresh(refreshToken string) (refreshClaims, error) {
	var claims refreshClaims
	parsed, err := jwt.ParseWithClaims(refreshToken, &claims, func(tok *jwt.Token) (any, error) {
		if typ, _ := tok.Header["typ"].(string); typ != refreshTokenType {
			return nil, errors.New("wrong typ")
		}
		kid, _ := tok.Header["kid"].(string)
		key, ok := m.keyring.refreshKey(kid)
		if !ok {
			return nil, errors.New("unknown kid")
		}
		return key, nil
	},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(m.issuer),
		jwt.WithAudience(refreshAudience),
		jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(m.clock.Now),
	)
	if err != nil {
		return refreshClaims{}, errors.PrefixError(err, "invalid refresh token")
	}
	if !parsed.Valid {
		return refreshClaims{}, errors.New("invalid refresh token")
	}
	return claims, nil
}

func checkWorkload(claims refreshClaims, app api.AppConfig, workload k8sapp.WorkloadRef) error {
	if claims.Kbc.AppID != workload.AppID.String() || claims.Kbc.AppID != app.ID.String() || claims.Kbc.ProjectID != app.ProjectID {
		return errors.New("refresh token issued for another app")
	}
	if claims.Kbc.Sandbox != workload.SandboxName {
		return errors.New("refresh token issued for another workload of the app")
	}
	return nil
}

// checkProvider requires that the provider which authenticated the user still guards a path of the app, with the same identity provider.
func checkProvider(claims refreshClaims, app api.AppConfig) error {
	id := provider.ID(claims.Kbc.AuthProviderID)
	if !slices.ContainsFunc(app.AuthProviders, func(p provider.Provider) bool { return p.ID() == id }) {
		return errors.New("auth provider no longer configured")
	}
	oauth, ok := oauthProvider(app, id)
	if !ok || oauth.IdentityProviderURL() != claims.IDP {
		return errors.New("auth provider changed")
	}
	allowed, err := allowedGroups(oauth)
	if err != nil {
		return err
	}
	if len(allowed) > 0 && len(matchingGroups(claims.Kbc.MatchedGroups, allowed)) == 0 {
		return errors.New("user is in none of the allowed groups")
	}
	requiresLogin := slices.ContainsFunc(app.AuthRules, func(rule api.Rule) bool {
		return (rule.AuthRequired == nil || *rule.AuthRequired) && slices.Contains(rule.Auth, id)
	})
	if !requiresLogin {
		return errors.New("auth provider no longer required")
	}
	return nil
}

// checkLoginSession refuses a refresh token whose login session was signed out, expired or ended by a refused IdP refresh.
func (m *Minter) checkLoginSession(ctx context.Context, key string) error {
	if key == "" {
		return errors.New("refresh token has no login session")
	}
	ttl, err := m.store.PTTL(ctx, key).Result()
	if err != nil {
		return errors.PrefixError(ErrSessionStoreUnavailable, err.Error())
	}
	if ttl == missingKeyTTL {
		return errors.New("login session ended")
	}
	return nil
}
