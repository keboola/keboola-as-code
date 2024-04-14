package api

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/request"
	"github.com/pquerna/cachecontrol/cacheobject"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
)

type AppID string

type AppProxyConfig struct {
	ID             AppID              `json:"-"`
	Name           string             `json:"name"`
	UpstreamAppURL string             `json:"upstreamAppUrl"`
	AuthProviders  provider.Providers `json:"authProviders"`
	AuthRules      []Rule             `json:"authRules"`
	eTag           string
	modified       bool
	maxAge         time.Duration
}

func (c AppID) String() string {
	return string(c)
}

func (c *AppProxyConfig) IsModified() bool {
	return c.modified
}

// GetAppProxyConfig loads proxy configuration for the specified app.
// eTag is used to detect modifications, if the eTag doesn't match, the AppProxyConfig.IsModified method returns true.
func (a *API) GetAppProxyConfig(appID AppID, eTag string) request.APIRequest[*AppProxyConfig] {
	result := &AppProxyConfig{}
	return request.NewAPIRequest(result, a.newRequest().
		WithResult(result).
		WithGet("apps/{appId}/proxy-config").
		AndPathParam("appId", appID.String()).
		AndHeader("If-None-Match", eTag).
		WithOnSuccess(func(ctx context.Context, response request.HTTPResponse) error {
			// Add app id to the result
			result.ID = appID

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
