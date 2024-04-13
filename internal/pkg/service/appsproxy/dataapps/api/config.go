package api

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/request"
	"github.com/pquerna/cachecontrol/cacheobject"
)

const (
	OIDCProvider = ProviderType("oidc")
	PathPrefix   = RuleType("pathPrefix")
)

type AppProxyConfig struct {
	ID             string         `json:"-"`
	Name           string         `json:"name"`
	UpstreamAppURL string         `json:"upstreamAppUrl"`
	AuthProviders  []AuthProvider `json:"authProviders"`
	AuthRules      []AuthRule     `json:"authRules"`
	eTag           string
	modified       bool
	maxAge         time.Duration
}

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

type AuthRule struct {
	Type         RuleType `json:"type"`
	Value        string   `json:"value"`
	Auth         []string `json:"auth"`
	AuthRequired *bool    `json:"authRequired"`
}

type ProviderType string

type RuleType string

// GetAppProxyConfig loads proxy configuration for the specified app.
// eTag is used to detect modifications, if the eTag doesn't match, the AppProxyConfig.IsModified method returns true.
func (a *API) GetAppProxyConfig(appID string, eTag string) request.APIRequest[*AppProxyConfig] {
	result := &AppProxyConfig{}
	return request.NewAPIRequest(result, a.newRequest().
		WithResult(result).
		WithGet("apps/{appId}/proxy-config").
		AndPathParam("appId", appID).
		AndHeader("If-None-Match", eTag).
		WithOnSuccess(func(ctx context.Context, response request.HTTPResponse) error {
			// Add app id to the result
			result.ID = appID

			// Use provider id as fallback until name is added to Sandboxes API
			for i, provider := range result.AuthProviders {
				if provider.Name == "" {
					result.AuthProviders[i].Name = provider.ID
				}
			}

			// Add ETag to result
			result.eTag = response.ResponseHeader().Get("ETag")

			// Process Cache-Control header
			cacheControl := response.ResponseHeader().Get("Cache-Control")
			if cacheControl == "" {
				return nil
			}

			cacheDirectives, err := cacheobject.ParseResponseCacheControl(cacheControl)
			if err != nil {
				return err
			}

			if !cacheDirectives.NoStore && cacheDirectives.NoCache == nil {
				result.maxAge = time.Second * time.Duration(cacheDirectives.MaxAge)
			}

			result.modified = result.eTag != eTag

			return nil
		}),
	)
}

func (c *AppProxyConfig) IsModified() bool {
	return c.modified
}
