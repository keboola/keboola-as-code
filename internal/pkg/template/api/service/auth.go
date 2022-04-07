package service

import (
	"context"
	"fmt"

	"goa.design/goa/v3/security"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/template/api/dependencies"
)

func (s *service) APIKeyAuth(ctx context.Context, tokenStr string, scheme *security.APIKeyScheme) (context.Context, error) {
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
		d = d.WithLoggerPrefix(fmt.Sprintf("[project=%d][token=%s]", token.ProjectId(), token.Id))

		// Add tags to DD span
		if span, ok := tracer.SpanFromContext(ctx); ok {
			span.SetTag("storage.project.id", token.ProjectId())
			span.SetTag("storage.token.id", token.Id)
		}

		// Update context
		return context.WithValue(ctx, dependencies.CtxKey, d), nil
	}

	panic(fmt.Errorf("unexpected security scheme: %#v", scheme))
}
