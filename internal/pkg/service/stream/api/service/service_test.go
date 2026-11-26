package service

import (
	"context"
	"testing"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/stretchr/testify/assert"

	svcerrors "github.com/keboola/keboola-as-code/internal/pkg/service/common/errors"
	api "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// tokenOnlyScope gives a handler only the Storage API token. Every other dependency is nil,
// so a handler that passes the role check panics on its next dependency call.
type tokenOnlyScope struct {
	dependencies.SinkRequestScope
	token keboola.Token
}

func (s tokenOnlyScope) StorageAPIToken() keboola.Token {
	return s.token
}

type writeHandler struct {
	name string
	call func(ctx context.Context, svc *service, d tokenOnlyScope) error
}

func writeHandlers() []writeHandler {
	return []writeHandler{
		{"CreateSource", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.CreateSource(ctx, d, &api.CreateSourcePayload{})
			return err
		}},
		{"UpdateSource", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.UpdateSource(ctx, d, &api.UpdateSourcePayload{})
			return err
		}},
		{"DeleteSource", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.DeleteSource(ctx, d, &api.DeleteSourcePayload{})
			return err
		}},
		{"UpdateSourceSettings", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.UpdateSourceSettings(ctx, d, &api.UpdateSourceSettingsPayload{})
			return err
		}},
		{"SourceStatisticsClear", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			return svc.SourceStatisticsClear(ctx, d, &api.SourceStatisticsClearPayload{})
		}},
		{"DisableSource", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.DisableSource(ctx, d, &api.DisableSourcePayload{})
			return err
		}},
		{"EnableSource", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.EnableSource(ctx, d, &api.EnableSourcePayload{})
			return err
		}},
		{"RotateSourceSecret", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.RotateSourceSecret(ctx, d, &api.RotateSourceSecretPayload{})
			return err
		}},
		{"UndeleteSource", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.UndeleteSource(ctx, d, &api.UndeleteSourcePayload{})
			return err
		}},
		{"RollbackSourceVersion", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.RollbackSourceVersion(ctx, d, &api.RollbackSourceVersionPayload{})
			return err
		}},
		{"CreateSink", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.CreateSink(ctx, d, &api.CreateSinkPayload{})
			return err
		}},
		{"UpdateSink", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.UpdateSink(ctx, d, &api.UpdateSinkPayload{})
			return err
		}},
		{"DeleteSink", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.DeleteSink(ctx, d, &api.DeleteSinkPayload{})
			return err
		}},
		{"UpdateSinkSettings", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.UpdateSinkSettings(ctx, d, &api.UpdateSinkSettingsPayload{})
			return err
		}},
		{"SinkStatisticsClear", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			return svc.SinkStatisticsClear(ctx, d, &api.SinkStatisticsClearPayload{})
		}},
		{"DisableSink", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.DisableSink(ctx, d, &api.DisableSinkPayload{})
			return err
		}},
		{"EnableSink", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.EnableSink(ctx, d, &api.EnableSinkPayload{})
			return err
		}},
		{"UndeleteSink", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.UndeleteSink(ctx, d, &api.UndeleteSinkPayload{})
			return err
		}},
		{"RollbackSinkVersion", func(ctx context.Context, svc *service, d tokenOnlyScope) error {
			_, err := svc.RollbackSinkVersion(ctx, d, &api.RollbackSinkVersionPayload{})
			return err
		}},
	}
}

// callWriteHandler turns the panic of a handler that got past the role check into an error.
func callWriteHandler(ctx context.Context, h writeHandler, svc *service, d tokenOnlyScope) (err error) {
	defer func() {
		if recover() != nil {
			err = errors.Errorf("%s passed the role check", h.name)
		}
	}()
	return h.call(ctx, svc, d)
}

func TestWriteHandlersCheckRole(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		admin   *keboola.TokenAdmin
		allowed bool
	}{
		{name: "admin", admin: &keboola.TokenAdmin{Role: "admin"}, allowed: true},
		{name: "share", admin: &keboola.TokenAdmin{Role: "share"}, allowed: true},
		{name: "readOnly", admin: &keboola.TokenAdmin{Role: "readOnly"}, allowed: false},
		{name: "guest", admin: &keboola.TokenAdmin{Role: "guest"}, allowed: false},
		{name: "empty role", admin: &keboola.TokenAdmin{}, allowed: false},
		{name: "not a master token", admin: nil, allowed: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			svc := &service{adminError: errors.New("only admin token can do write operations on streams")}
			d := tokenOnlyScope{token: keboola.Token{Admin: tt.admin}}
			for _, h := range writeHandlers() {
				err := callWriteHandler(t.Context(), h, svc, d)
				forbidden := errors.As(err, &svcerrors.ForbiddenError{})
				assert.Equal(t, !tt.allowed, forbidden, "%s: unexpected result %v", h.name, err)
			}
		})
	}
}
