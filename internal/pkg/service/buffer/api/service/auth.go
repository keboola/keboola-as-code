package service

import (
	"context"

	"goa.design/goa/v3/security"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/dependencies"
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

		// Update context
		return context.WithValue(ctx, dependencies.ForProjectRequestCtxKey, projectDeps), nil
	}

	panic(errors.Errorf("unexpected security scheme: %#v", scheme))
}
