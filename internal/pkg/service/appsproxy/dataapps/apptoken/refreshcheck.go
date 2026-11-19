package apptoken

import (
	"context"
	"slices"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/api"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/k8sapp"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// missingKeyTTL is what PTTL answers for a key that does not exist.
const missingKeyTTL = -2 * time.Nanosecond

// ErrSessionStoreUnavailable means the login session of a refresh token could not be checked.
var ErrSessionStoreUnavailable = errors.New("login session store unavailable")

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
