package provider

import (
	"slices"
	"strings"

	proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/providers"
)

type OIDC struct {
	Base
	ClientID     string    `json:"clientId"`
	ClientSecret string    `json:"clientSecret"`
	IssuerURL    string    `json:"issuerUrl"`
	LogoutURL    string    `json:"logoutUrl"`
	AllowedRoles *[]string `json:"allowedRoles"`
	// Scopes are requested in addition to DefaultOIDCScope, e.g. "groups" for an IdP
	// that puts the groups claim in the ID token only when that scope is requested.
	Scopes []string `json:"scopes"`
}

// DefaultOIDCScope never contains "groups": the groups claim does not depend on that scope
// at most IdPs, and an authorization server without it (Okta default) rejects the login.
const DefaultOIDCScope = "openid email profile"

func (v OIDC) ProxyProviderOptions() (proxyOptions.Provider, error) {
	p := proxyOptions.Provider{
		ID:                  v.ID().String(),
		Type:                proxyOptions.OIDCProvider,
		Name:                v.Name(),
		CodeChallengeMethod: providers.CodeChallengeMethodS256,
		Scope:               v.scope(),
		ClientID:            v.ClientID,
		ClientSecret:        v.ClientSecret,
		BackendLogoutURL:    v.LogoutURL,
		LoginURLParameters: []proxyOptions.LoginURLParameter{
			{
				// https://openid.net/specs/openid-connect-core-1_0.html#AuthRequest
				// See "prompt" options: none, login, consent, select_account
				Name:    "prompt", // the user can choose a different account on each login attempt
				Default: []string{"select_account"},
			},
		},
		// The subject claim identifies the user without naming them. It is
		// stable across e-mail and display-name changes, which the email claim
		// used as the user id is not. Only reachable when the provider issues
		// an ID token, so GitHub — which does not — yields nothing.
		// email_verified feeds the app user token: oauth2-proxy admits a login
		// whose ID token omits it, so only an explicit true makes the email trusted.
		AdditionalClaims: []string{"sub", "email_verified"},
		OIDCConfig: proxyOptions.OIDCOptions{
			IssuerURL:      v.IssuerURL,
			EmailClaim:     proxyOptions.OIDCEmailClaim,
			GroupsClaim:    proxyOptions.OIDCGroupsClaim,
			AudienceClaims: proxyOptions.OIDCAudienceClaims,
			UserIDClaim:    proxyOptions.OIDCEmailClaim,
		},
	}

	// AllowedRoles nil means there is no role requirement.
	// An empty array is not passed on: oauth2-proxy reads empty AllowedGroups as "allow all", see DeniesAll.
	if v.AllowedRoles != nil && len(*v.AllowedRoles) > 0 {
		p.AllowedGroups = *v.AllowedRoles
	}

	return p, nil
}

// DeniesAll reports an empty array of allowed roles: every user may log in, none may reach the app.
func (v OIDC) DeniesAll() bool {
	return v.AllowedRoles != nil && len(*v.AllowedRoles) == 0
}

func (v OIDC) scope() string {
	scopes := strings.Fields(DefaultOIDCScope)
	for _, scope := range strings.Fields(strings.Join(v.Scopes, " ")) {
		if !slices.Contains(scopes, scope) {
			scopes = append(scopes, scope)
		}
	}
	return strings.Join(scopes, " ")
}

func (v OIDC) RefreshTokensSingleUse() bool {
	return false
}

func (v OIDC) IdentityProviderURL() string {
	return strings.TrimRight(v.IssuerURL, "/")
}
