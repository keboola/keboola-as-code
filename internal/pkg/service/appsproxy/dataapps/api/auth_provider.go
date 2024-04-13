package api

const (
	OIDCProvider = ProviderType("oidc")
)

type ProviderType string

type AuthProvider struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	Type         ProviderType `json:"type"`
	ClientID     string       `json:"clientId"`
	ClientSecret string       `json:"clientSecret"`
	IssuerURL    string       `json:"issuerUrl"`
	LogoutURL    string       `json:"logoutUrl"`
	AllowedRoles *[]string    `json:"allowedRoles"`
}
