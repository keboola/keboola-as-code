package apptoken

import (
	"github.com/golang-jwt/jwt/v5"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/config"
)

const (
	RefreshHeaderName   = "X-Kbc-App-User-Refresh-Token" //nolint:gosec // G101: a header name, not a credential
	PublicURLHeaderName = "X-Kbc-App-Public-Url"
	RefreshPath         = config.InternalPrefix + "/app-user-token"

	refreshTokenType = "kbc-app-user-refresh+jwt" //nolint:gosec // G101: the JOSE typ value, not a credential
	refreshAudience  = "apps-proxy"
)

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
