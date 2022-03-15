package service

import (
	"context"
	"fmt"

	"goa.design/goa/v3/security"

	"github.com/keboola/keboola-as-code/internal/pkg/template/api/dependencies"
)

func (s *Service) APIKeyAuth(ctx context.Context, tokenStr string, scheme *security.APIKeyScheme) (context.Context, error) {
	if scheme.Name == "storage-api-token" {
		// Get API
		api, err := s.dependencies.StorageApi()
		if err != nil {
			return ctx, err
		}

		// Verify token
		token, err := api.GetToken(tokenStr)
		if err != nil {
			return ctx, err
		}

		// Modify dependencies
		d, err := s.dependencies.WithStorageApi(api.WithToken(token))
		if err != nil {
			return nil, err
		}

		// Modify logger
		d, err = d.WithLoggerPrefix(fmt.Sprintf("[project=%d][token=%s]", token.ProjectId(), token.Id))
		if err != nil {
			return nil, err
		}

		// Update context
		return context.WithValue(ctx, dependencies.CtxKey, d), nil
	}

	panic(fmt.Errorf("unexpected security scheme: %#v", scheme))
}
