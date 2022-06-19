package service

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"goa.design/goa/v3/security"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/keboola/keboola-as-code/internal/pkg/api/server/templates/dependencies"
)

func (s *service) APIKeyAuth(ctx context.Context, tokenStr string, scheme *security.APIKeyScheme) (context.Context, error) {
	if scheme.Name == "storage-api-token" {
		d := ctx.Value(dependencies.CtxKey).(dependencies.Container)

		// Get API
		storageApiClient, err := d.StorageApiClient()
		if err != nil {
			return ctx, err
		}

		// Verify token
		token, err := storageapi.VerifyTokenRequest(tokenStr).Send(ctx, storageApiClient)
		if err != nil {
			return ctx, err
		}

		// Modify dependencies
		d, err = d.WithStorageApiClient(storageapi.ClientWithToken(storageApiClient.(client.Client), token.Token), token)
		if err != nil {
			return nil, err
		}

		// Modify logger
		d = d.WithLoggerPrefix(fmt.Sprintf("[project=%d][token=%s]", token.ProjectID(), token.ID))

		// Add tags to DD span
		if span, ok := tracer.SpanFromContext(ctx); ok {
			span.SetTag("storage.project.id", token.ProjectID())
			span.SetTag("storage.token.id", token.ID)
		}

		// Update context
		return context.WithValue(ctx, dependencies.CtxKey, d), nil
	}

	panic(fmt.Errorf("unexpected security scheme: %#v", scheme))
}
