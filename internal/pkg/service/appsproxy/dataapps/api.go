package dataapps

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/request"
	"github.com/pquerna/cachecontrol/cacheobject"
)

type AppProxyConfig struct {
	ID             string         `json:"-"`
	Name           string         `json:"name"`
	UpstreamAppURL string         `json:"upstreamAppUrl"`
	AuthProviders  []AuthProvider `json:"authProviders"`
	AuthRules      []AuthRule     `json:"authRules"`
	eTag           string
	maxAge         time.Duration
}

type ProviderType string

const OIDCProvider = ProviderType("oidc")

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

type RuleType string

const PathPrefix = RuleType("pathPrefix")

type AuthRule struct {
	Type         RuleType `json:"type"`
	Value        string   `json:"value"`
	Auth         []string `json:"auth"`
	AuthRequired *bool    `json:"authRequired"`
}

type NotifyBody struct {
	LastRequestTimestamp string `json:"lastRequestTimestamp"`
}

func PatchNotifyAppUsage(sender request.Sender, appID string, lastRequestTimestamp time.Time) request.APIRequest[request.NoResult] {
	body := NotifyBody{
		LastRequestTimestamp: lastRequestTimestamp.Format(time.RFC3339),
	}
	req := request.NewHTTPRequest(sender).
		WithError(&SandboxesError{}).
		WithPatch("apps/{appId}").
		AndPathParam("appId", appID).
		WithJSONBody(body)
	return request.NewAPIRequest(request.NoResult{}, req)
}

func GetAppProxyConfig(sender request.Sender, appID string, eTag string) request.APIRequest[*AppProxyConfig] {
	result := &AppProxyConfig{}
	req := request.NewHTTPRequest(sender).
		WithError(&SandboxesError{}).
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

			if cacheDirectives.NoStore || cacheDirectives.NoCache != nil {
				return nil
			}

			result.maxAge = time.Second * time.Duration(cacheDirectives.MaxAge)
			return nil
		})
	return request.NewAPIRequest(result, req)
}
