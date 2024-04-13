package api

type OIDCProvider struct {
	BaseProvider
	ClientID     string    `json:"clientId"`
	ClientSecret string    `json:"clientSecret"`
	IssuerURL    string    `json:"issuerUrl"`
	LogoutURL    string    `json:"logoutUrl"`
	AllowedRoles *[]string `json:"allowedRoles"`
}
