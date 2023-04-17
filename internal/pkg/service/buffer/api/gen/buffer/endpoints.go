// Code generated by goa v3.11.3, DO NOT EDIT.
//
// buffer endpoints
//
// Command:
// $ goa gen github.com/keboola/keboola-as-code/api/buffer --output
// ./internal/pkg/service/buffer/api

package buffer

import (
	"context"
	"io"

	dependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
	goa "goa.design/goa/v3/pkg"
	"goa.design/goa/v3/security"
)

// Endpoints wraps the "buffer" service endpoints.
type Endpoints struct {
	APIRootIndex          goa.Endpoint
	APIVersionIndex       goa.Endpoint
	HealthCheck           goa.Endpoint
	CreateReceiver        goa.Endpoint
	UpdateReceiver        goa.Endpoint
	ListReceivers         goa.Endpoint
	GetReceiver           goa.Endpoint
	DeleteReceiver        goa.Endpoint
	RefreshReceiverTokens goa.Endpoint
	CreateExport          goa.Endpoint
	GetExport             goa.Endpoint
	ListExports           goa.Endpoint
	UpdateExport          goa.Endpoint
	DeleteExport          goa.Endpoint
	Import                goa.Endpoint
	GetTask               goa.Endpoint
}

// ImportRequestData holds both the payload and the HTTP request body reader of
// the "Import" method.
type ImportRequestData struct {
	// Payload is the method payload.
	Payload *ImportPayload
	// Body streams the HTTP request body.
	Body io.ReadCloser
}

// NewEndpoints wraps the methods of the "buffer" service with endpoints.
func NewEndpoints(s Service) *Endpoints {
	// Casting service to Auther interface
	a := s.(Auther)
	return &Endpoints{
		APIRootIndex:          NewAPIRootIndexEndpoint(s),
		APIVersionIndex:       NewAPIVersionIndexEndpoint(s),
		HealthCheck:           NewHealthCheckEndpoint(s),
		CreateReceiver:        NewCreateReceiverEndpoint(s, a.APIKeyAuth),
		UpdateReceiver:        NewUpdateReceiverEndpoint(s, a.APIKeyAuth),
		ListReceivers:         NewListReceiversEndpoint(s, a.APIKeyAuth),
		GetReceiver:           NewGetReceiverEndpoint(s, a.APIKeyAuth),
		DeleteReceiver:        NewDeleteReceiverEndpoint(s, a.APIKeyAuth),
		RefreshReceiverTokens: NewRefreshReceiverTokensEndpoint(s, a.APIKeyAuth),
		CreateExport:          NewCreateExportEndpoint(s, a.APIKeyAuth),
		GetExport:             NewGetExportEndpoint(s, a.APIKeyAuth),
		ListExports:           NewListExportsEndpoint(s, a.APIKeyAuth),
		UpdateExport:          NewUpdateExportEndpoint(s, a.APIKeyAuth),
		DeleteExport:          NewDeleteExportEndpoint(s, a.APIKeyAuth),
		Import:                NewImportEndpoint(s),
		GetTask:               NewGetTaskEndpoint(s, a.APIKeyAuth),
	}
}

// Use applies the given middleware to all the "buffer" service endpoints.
func (e *Endpoints) Use(m func(goa.Endpoint) goa.Endpoint) {
	e.APIRootIndex = m(e.APIRootIndex)
	e.APIVersionIndex = m(e.APIVersionIndex)
	e.HealthCheck = m(e.HealthCheck)
	e.CreateReceiver = m(e.CreateReceiver)
	e.UpdateReceiver = m(e.UpdateReceiver)
	e.ListReceivers = m(e.ListReceivers)
	e.GetReceiver = m(e.GetReceiver)
	e.DeleteReceiver = m(e.DeleteReceiver)
	e.RefreshReceiverTokens = m(e.RefreshReceiverTokens)
	e.CreateExport = m(e.CreateExport)
	e.GetExport = m(e.GetExport)
	e.ListExports = m(e.ListExports)
	e.UpdateExport = m(e.UpdateExport)
	e.DeleteExport = m(e.DeleteExport)
	e.Import = m(e.Import)
	e.GetTask = m(e.GetTask)
}

// NewAPIRootIndexEndpoint returns an endpoint function that calls the method
// "ApiRootIndex" of service "buffer".
func NewAPIRootIndexEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		deps := ctx.Value(dependencies.ForPublicRequestCtxKey).(dependencies.ForPublicRequest)
		return nil, s.APIRootIndex(deps)
	}
}

// NewAPIVersionIndexEndpoint returns an endpoint function that calls the
// method "ApiVersionIndex" of service "buffer".
func NewAPIVersionIndexEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		deps := ctx.Value(dependencies.ForPublicRequestCtxKey).(dependencies.ForPublicRequest)
		return s.APIVersionIndex(deps)
	}
}

// NewHealthCheckEndpoint returns an endpoint function that calls the method
// "HealthCheck" of service "buffer".
func NewHealthCheckEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		deps := ctx.Value(dependencies.ForPublicRequestCtxKey).(dependencies.ForPublicRequest)
		return s.HealthCheck(deps)
	}
}

// NewCreateReceiverEndpoint returns an endpoint function that calls the method
// "CreateReceiver" of service "buffer".
func NewCreateReceiverEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*CreateReceiverPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return s.CreateReceiver(deps, p)
	}
}

// NewUpdateReceiverEndpoint returns an endpoint function that calls the method
// "UpdateReceiver" of service "buffer".
func NewUpdateReceiverEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*UpdateReceiverPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return s.UpdateReceiver(deps, p)
	}
}

// NewListReceiversEndpoint returns an endpoint function that calls the method
// "ListReceivers" of service "buffer".
func NewListReceiversEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*ListReceiversPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return s.ListReceivers(deps, p)
	}
}

// NewGetReceiverEndpoint returns an endpoint function that calls the method
// "GetReceiver" of service "buffer".
func NewGetReceiverEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*GetReceiverPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return s.GetReceiver(deps, p)
	}
}

// NewDeleteReceiverEndpoint returns an endpoint function that calls the method
// "DeleteReceiver" of service "buffer".
func NewDeleteReceiverEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*DeleteReceiverPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return nil, s.DeleteReceiver(deps, p)
	}
}

// NewRefreshReceiverTokensEndpoint returns an endpoint function that calls the
// method "RefreshReceiverTokens" of service "buffer".
func NewRefreshReceiverTokensEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*RefreshReceiverTokensPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return s.RefreshReceiverTokens(deps, p)
	}
}

// NewCreateExportEndpoint returns an endpoint function that calls the method
// "CreateExport" of service "buffer".
func NewCreateExportEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*CreateExportPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return s.CreateExport(deps, p)
	}
}

// NewGetExportEndpoint returns an endpoint function that calls the method
// "GetExport" of service "buffer".
func NewGetExportEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*GetExportPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return s.GetExport(deps, p)
	}
}

// NewListExportsEndpoint returns an endpoint function that calls the method
// "ListExports" of service "buffer".
func NewListExportsEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*ListExportsPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return s.ListExports(deps, p)
	}
}

// NewUpdateExportEndpoint returns an endpoint function that calls the method
// "UpdateExport" of service "buffer".
func NewUpdateExportEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*UpdateExportPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return s.UpdateExport(deps, p)
	}
}

// NewDeleteExportEndpoint returns an endpoint function that calls the method
// "DeleteExport" of service "buffer".
func NewDeleteExportEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*DeleteExportPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return nil, s.DeleteExport(deps, p)
	}
}

// NewImportEndpoint returns an endpoint function that calls the method
// "Import" of service "buffer".
func NewImportEndpoint(s Service) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		ep := req.(*ImportRequestData)
		deps := ctx.Value(dependencies.ForPublicRequestCtxKey).(dependencies.ForPublicRequest)
		return nil, s.Import(deps, ep.Payload, ep.Body)
	}
}

// NewGetTaskEndpoint returns an endpoint function that calls the method
// "GetTask" of service "buffer".
func NewGetTaskEndpoint(s Service, authAPIKeyFn security.AuthAPIKeyFunc) goa.Endpoint {
	return func(ctx context.Context, req any) (any, error) {
		p := req.(*GetTaskPayload)
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
		deps := ctx.Value(dependencies.ForProjectRequestCtxKey).(dependencies.ForProjectRequest)
		return s.GetTask(deps, p)
	}
}
