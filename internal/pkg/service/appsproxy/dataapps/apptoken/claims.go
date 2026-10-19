package apptoken

import (
	"github.com/golang-jwt/jwt/v5"
)

const (
	HeaderName = "X-Kbc-App-User-Token"
	tokenType  = "kbc-app-user+jwt" //nolint:gosec // G101: the JOSE typ value, not a credential
	audience   = "keboola"
)

// Claims is written by hand, not built on jwt.RegisteredClaims: that type
// encodes aud as an array, and verifiers expect the plain string "keboola".
type Claims struct {
	Issuer    string           `json:"iss"`
	Audience  string           `json:"aud"`
	Subject   string           `json:"sub"`
	IDP       string           `json:"idp"`
	Email     string           `json:"email,omitempty"`
	Groups    []string         `json:"groups,omitempty"`
	Kbc       KbcClaims        `json:"kbc"`
	ID        string           `json:"jti"`
	SID       string           `json:"sid,omitempty"`
	IssuedAt  *jwt.NumericDate `json:"iat"`
	ExpiresAt *jwt.NumericDate `json:"exp"`
}

//nolint:tagliatelle // the claim names are part of the token contract with the verifiers.
type KbcClaims struct {
	ProjectID   string `json:"project_id"`
	AppID       string `json:"app_id"`
	WorkspaceID string `json:"workspace_id,omitempty"`
}

func (c Claims) GetExpirationTime() (*jwt.NumericDate, error) {
	return c.ExpiresAt, nil
}

func (c Claims) GetIssuedAt() (*jwt.NumericDate, error) {
	return c.IssuedAt, nil
}

func (c Claims) GetNotBefore() (*jwt.NumericDate, error) {
	return nil, nil
}

func (c Claims) GetIssuer() (string, error) {
	return c.Issuer, nil
}

func (c Claims) GetSubject() (string, error) {
	return c.Subject, nil
}

func (c Claims) GetAudience() (jwt.ClaimStrings, error) {
	return jwt.ClaimStrings{c.Audience}, nil
}
