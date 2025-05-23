// Code generated by goa v3.20.1, DO NOT EDIT.
//
// apps-proxy endpoints
//
// Command:
// $ goa gen github.com/keboola/keboola-as-code/api/appsproxy --output
// ./internal/pkg/service/appsproxy/api

package appsproxy

import (
	"context"

	dependencies "github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	goa "goa.design/goa/v3/pkg"
	"goa.design/goa/v3/security"
)

// Endpoints wraps the "apps-proxy" service endpoints.
type Endpoints struct {
	APIRootIndex    goa.Endpoint
	APIVersionIndex goa.Endpoint
	HealthCheck     goa.Endpoint
	Validate        goa.Endpoint
}

// NewEndpoints wraps the methods of the "apps-proxy" service with endpoints.
func NewEndpoints(s Service) *Endpoints {
	// Casting service to Auther interface
	a := s.(Auther)
	return &Endpoints{
		APIRootIndex:    NewAPIRootIndexEndpoint(s),
		APIVersionIndex: NewAPIVersionIndexEndpoint(s),
		HealthCheck:     NewHealthCheckEndpoint(s),
		Validate:        NewValidateEndpoint(s, a.APIKeyAuth),
	}
}

// Use applies the given middleware to all the "apps-proxy" service endpoints.
func (e *Endpoints) Use(m func(goa.Endpoint) goa.Endpoint) {
	e.APIRootIndex = m(e.APIRootIndex)
	e.APIVersionIndex = m(e.APIVersionIndex)
	e.HealthCheck = m(e.HealthCheck)
	e.Validate = m(e.Validate)
}

// NewAPIRootIndexEndpoint returns an endpoint function that calls the method
// "ApiRootIndex" of service "apps-proxy".
func NewAPIRootIndexEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		deps := ctx.Value(dependencies.PublicRequestScopeCtxKey).(dependencies.PublicRequestScope)
		return nil, s.APIRootIndex(ctx, deps)
	}
}

// NewAPIVersionIndexEndpoint returns an endpoint function that calls the
// method "ApiVersionIndex" of service "apps-proxy".
func NewAPIVersionIndexEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		deps := ctx.Value(dependencies.PublicRequestScopeCtxKey).(dependencies.PublicRequestScope)
		return s.APIVersionIndex(ctx, deps)
	}
}

// NewHealthCheckEndpoint returns an endpoint function that calls the method
// "HealthCheck" of service "apps-proxy".
func NewHealthCheckEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		deps := ctx.Value(dependencies.PublicRequestScopeCtxKey).(dependencies.PublicRequestScope)
		return s.HealthCheck(ctx, deps)
	}
}

// NewValidateEndpoint returns an endpoint function that calls the method
// "Validate" of service "apps-proxy".
func NewValidateEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*ValidatePayload)
		var err error
		sc := security.APIKeyScheme{
			Name:           "storage-api-token",
			Scopes:         []string{},
			RequiredScopes: []string{},
		}
		ctx, err = authAPIKeyFn(ctx, p.StorageAPIToken, &sc)
		if err != nil {
			return nil, err
		}
		deps := ctx.Value(dependencies.ProjectRequestScopeCtxKey).(dependencies.ProjectRequestScope)
		return s.Validate(ctx, deps, p)
	}
}
