package provider

import (
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
}

func (v OIDC) ToProxyProvider() (proxyOptions.Provider, error) {
	p := proxyOptions.Provider{
		ID:                  v.ID().String(),
		Type:                proxyOptions.OIDCProvider,
		Name:                v.Name(),
		CodeChallengeMethod: providers.CodeChallengeMethodS256,
		ClientID:            v.ClientID,
		ClientSecret:        v.ClientSecret,
		BackendLogoutURL:    v.LogoutURL,
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
		return proxyOptions.Provider{}, errors.Errorf(`unexpected empty array of allowed roles for app "%s" "%s"`, v.ID(), v.Name())
	}

	return p, nil
}
