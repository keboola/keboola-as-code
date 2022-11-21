package service

import (
	"context"

	"goa.design/goa/v3/security"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/service/templates/api/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *service) APIKeyAuth(ctx context.Context, tokenStr string, scheme *security.APIKeyScheme) (context.Context, error) {
	if scheme.Name == "storage-api-token" {
		// Create project dependencies for the API request, it includes authentication
		publicDeps := ctx.Value(dependencies.ForPublicRequestCtxKey).(dependencies.ForPublicRequest)
		projectDeps, err := dependencies.NewDepsForProjectRequest(publicDeps, ctx, tokenStr)
		if err != nil {
			return nil, err
		}

		// Add tags to DD span
		if span, ok := tracer.SpanFromContext(ctx); ok {
			span.SetTag("kac.project.id", projectDeps.ProjectID())
			span.SetTag("kac.storage.token.id", projectDeps.StorageApiTokenID())
		}

		// Update context
		return context.WithValue(ctx, dependencies.ForProjectRequestCtxKey, projectDeps), nil
	}

	panic(errors.Errorf("unexpected security scheme: %#v", scheme))
}
