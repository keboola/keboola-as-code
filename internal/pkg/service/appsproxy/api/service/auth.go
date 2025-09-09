package service

import (
	"context"

	"goa.design/goa/v3/security"

	"github.com/keboola/keboola-as-code/internal/pkg/service/appsproxy/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (s *service) APIKeyAuth(ctx context.Context, tokenStr string, scheme *security.APIKeyScheme) (context.Context, error) {
	if scheme.Name == "master-token" {
		// Create project dependencies for the API request, it includes authentication
		pubReqScp := ctx.Value(dependencies.PublicRequestScopeCtxKey).(dependencies.PublicRequestScope)
		prjReqScp, err := dependencies.NewProjectRequestScope(ctx, pubReqScp, tokenStr)
		if err != nil {
			return nil, err
		}

		// Update context
		return context.WithValue(ctx, dependencies.ProjectRequestScopeCtxKey, prjReqScp), nil
	}

	panic(errors.Errorf("unexpected security scheme: %#v", scheme))
}
