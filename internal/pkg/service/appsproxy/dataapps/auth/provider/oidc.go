package provider

type OIDC struct {
	Base
	ClientID     string    `json:"clientId"`
	ClientSecret string    `json:"clientSecret"`
	IssuerURL    string    `json:"issuerUrl"`
	LogoutURL    string    `json:"logoutUrl"`
	AllowedRoles *[]string `json:"allowedRoles"`
}
