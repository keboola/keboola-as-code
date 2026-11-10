package provider

import (
	"slices"
	"strings"

	sdk "github.com/keboola/keboola-sdk-go/v2/pkg/apptoken"
	proxyOptions "github.com/oauth2-proxy/oauth2-proxy/v7/pkg/apis/options"
	"github.com/oauth2-proxy/oauth2-proxy/v7/providers"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type OIDC struct {
	Base
	ClientID     string    `json:"clientId"`
	ClientSecret string    `json:"clientSecret"`
	IssuerURL    string    `json:"issuerUrl"`
	LogoutURL    string    `json:"logoutUrl"`
	AllowedRoles *[]string `json:"allowedRoles"`
	// AttributeClaims are the claims of the identity provider that the app owner allows as attributes of the user in the app user
	// token, in the order in which they are given. Rules of Query Service select readers by them. See ValidAttributeClaims.
	AttributeClaims []string `json:"attributeClaims"`
}

// ValidAttributeClaims returns the allowlist of attribute claims, or an error when it cannot be used: more names than a token
// carries, a name that has a wrong shape or is reserved (see sdk.ValidAttrName), or a repeated name. The error never names a
// claim. An empty allowlist is not an error.
func (v OIDC) ValidAttributeClaims() ([]string, error) {
	if len(v.AttributeClaims) == 0 {
		return nil, nil
	}
	if len(v.AttributeClaims) > sdk.MaxAttrNames {
		return nil, errors.New("too many attribute claims")
	}
	for i, name := range v.AttributeClaims {
		if !sdk.ValidAttrName(name) {
			return nil, errors.New("an attribute claim has a bad or reserved name")
		}
		if slices.Contains(v.AttributeClaims[:i], name) {
			return nil, errors.New("duplicate attribute claim")
		}
	}

	return slices.Clone(v.AttributeClaims), nil
}

func (v OIDC) ProxyProviderOptions() (proxyOptions.Provider, error) {
	// oauth2-proxy extracts these claims of the ID token once, at login, and keeps them in the session. The attribute claims are
	// extracted only when the allowlist is valid: apps-proxy does not need to refuse the provider for a bad one, it mints no attributes.
	additionalClaims := []string{"sub", "email_verified"}
	if attributeClaims, err := v.ValidAttributeClaims(); err == nil {
		additionalClaims = append(additionalClaims, attributeClaims...)
	}

	p := proxyOptions.Provider{
		ID:                  v.ID().String(),
		Type:                proxyOptions.OIDCProvider,
		Name:                v.Name(),
		CodeChallengeMethod: providers.CodeChallengeMethodS256,
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
		AdditionalClaims: additionalClaims,
		OIDCConfig: proxyOptions.OIDCOptions{
			IssuerURL:      v.IssuerURL,
			EmailClaim:     proxyOptions.OIDCEmailClaim,
			GroupsClaim:    proxyOptions.OIDCGroupsClaim,
			AudienceClaims: proxyOptions.OIDCAudienceClaims,
			UserIDClaim:    proxyOptions.OIDCEmailClaim,
		},
	}

	// AllowedRoles nil means there is no role requirement
	if v.AllowedRoles != nil {
		p.AllowedGroups = *v.AllowedRoles
	}

	// AllowedRoles empty array doesn't make sense
	if v.AllowedRoles != nil && len(*v.AllowedRoles) == 0 {
		return proxyOptions.Provider{}, errors.Errorf(`unexpected empty array of allowed roles for provider "%s"`, v.ID())
	}

	return p, nil
}

func (v OIDC) IdentityProviderURL() string {
	return strings.TrimRight(v.IssuerURL, "/")
}
