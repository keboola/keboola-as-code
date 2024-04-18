package api

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/keboola/go-client/pkg/request"
	"github.com/pquerna/cachecontrol/cacheobject"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dataapps/auth/provider"
)

const (
	// maxCacheExpiration is the maximum duration for which an old AppConfig of a data app is cached.
	maxCacheExpiration = time.Hour
	attrAppID          = "proxy.app.id"
	attrAppName        = "proxy.app.name"
	attrAppUpstream    = "proxy.app.upstream"
)

type AppID string

type AppConfig struct {
	ID             AppID              `json:"-"`
	Name           string             `json:"name"`
	UpstreamAppURL string             `json:"upstreamAppUrl"`
	AuthProviders  provider.Providers `json:"authProviders"`
	AuthRules      []Rule             `json:"authRules"`
	eTag           string
	maxAge         time.Duration
}

type NotModifiedError struct {
	MaxAge time.Duration
}

func (v NotModifiedError) Error() string {
	return "app proxy config: not modified"
}

func (c AppID) String() string {
	return string(c)
}

func (c AppConfig) Domain() string {
	if c.Name == "" {
		return c.ID.String()
	} else {
		return c.Name + "-" + c.ID.String()
	}
}

func (c AppConfig) ETag() string {
	return c.eTag
}

func (c AppConfig) MaxAge() time.Duration {
	return c.maxAge
}

func (c AppConfig) Telemetry() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(attrAppID, c.ID.String()),
		attribute.String(attrAppName, c.Name),
		attribute.String(attrAppUpstream, c.UpstreamAppURL),
	}
}

// GetAppConfig loads proxy configuration for the specified app.
// eTag is used to detect modifications, if the eTag match, the NotModifiedError is returned.
func (a *API) GetAppConfig(appID AppID, eTag string) request.APIRequest[*AppConfig] {
	result := &AppConfig{}
	return request.NewAPIRequest(result, a.newRequest().
		WithResult(result).
		WithGet("apps/{appId}/proxy-config").
		AndPathParam("appId", appID.String()).
		AndHeader("If-None-Match", eTag).
		WithOnSuccess(func(ctx context.Context, response request.HTTPResponse) error {
			// Process Cache-Control header
			var maxAge time.Duration
			if cacheControl := response.ResponseHeader().Get("Cache-Control"); cacheControl != "" {
				if cacheDirectives, err := cacheobject.ParseResponseCacheControl(cacheControl); err != nil {
					return err
				} else if cacheDirectives.MaxAge > 0 {
					maxAge = minDuration(maxCacheExpiration, time.Second*time.Duration(cacheDirectives.MaxAge))
				}
			}

			// Return specific error if there is no content, because StatusNotModified
			if response.StatusCode() == http.StatusNotModified {
				return NotModifiedError{MaxAge: maxAge}
			}

			// Add app id to the result
			result.ID = appID

			// Add ETag to result
			result.eTag = response.ResponseHeader().Get("ETag")

			// Add MaxAge
			result.maxAge = maxAge

			// Sort rules, longest value/path first
			slices.SortStableFunc(result.AuthRules, func(a, b Rule) int {
				return strings.Compare(b.Value, a.Value)
			})

			return nil
		}),
	)
}

func minDuration(durationA time.Duration, durationB time.Duration) time.Duration {
	if durationA <= durationB {
		return durationA
	}
	return durationB
}
