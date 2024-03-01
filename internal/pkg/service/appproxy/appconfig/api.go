package appconfig

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/keboola/go-client/pkg/request"
	"github.com/pquerna/cachecontrol/cacheobject"
)

type AppProxyConfig struct {
	ID              string         `json:"id"`
	Name            string         `json:"name"`
	UpstreamAppHost string         `json:"upstreamAppHost"`
	AuthProviders   []AuthProvider `json:"authProviders"`
	AuthRules       []AuthRule     `json:"authRules"`
	ETag            string         `json:"-"`
	maxAge          time.Duration
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
	AllowedRoles []string     `json:"allowedRoles"`
}

type RuleType string

const PathPrefix = RuleType("pathPrefix")

type AuthRule struct {
	Type  RuleType `json:"type"`
	Value string   `json:"value"`
	Auth  []string `json:"auth"`
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
			// Use id as fallback until name is added to Sandboxes API
			if result.Name == "" {
				result.Name = result.ID
			}
			for i, provider := range result.AuthProviders {
				if provider.Name == "" {
					result.AuthProviders[i].Name = provider.ID
				}
			}

			// Add ETag to result
			result.ETag = response.ResponseHeader().Get("ETag")

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

// SandboxesError represents the structure of API error.
type SandboxesError struct {
	Message     string `json:"error"`
	ExceptionID string `json:"exceptionId"`
	request     *http.Request
	response    *http.Response
}

func (e *SandboxesError) Error() string {
	return fmt.Sprintf("sandboxes api error[%d]: %s", e.StatusCode(), e.Message)
}

// ErrorName returns a human-readable name of the error.
func (e *SandboxesError) ErrorName() string {
	return http.StatusText(e.StatusCode())
}

// ErrorUserMessage returns error message for end user.
func (e *SandboxesError) ErrorUserMessage() string {
	return e.Message
}

// ErrorExceptionID returns exception ID to find details in logs.
func (e *SandboxesError) ErrorExceptionID() string {
	return e.ExceptionID
}

// StatusCode returns HTTP status code.
func (e *SandboxesError) StatusCode() int {
	return e.response.StatusCode
}

// SetRequest method allows injection of HTTP request to the error, it implements client.errorWithRequest.
func (e *SandboxesError) SetRequest(request *http.Request) {
	e.request = request
}

// SetResponse method allows injection of HTTP response to the error, it implements client.errorWithResponse.
func (e *SandboxesError) SetResponse(response *http.Response) {
	e.response = response
}
