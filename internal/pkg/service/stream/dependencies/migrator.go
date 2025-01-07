package dependencies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
)

// migratorScope implements MigratorScope interface.
type migratorScope struct {
	ServiceScope
}

func NewMigratorScope(ctx context.Context, svcScope ServiceScope, cfg config.Config) (v MigratorScope, err error) {
	return newMigratorScope(ctx, svcScope, cfg)
}

func NewMockedMigratorScope(tb testing.TB, ctx context.Context, opts ...dependencies.MockedOption) (MigratorScope, Mocked) {
	tb.Helper()
	return NewMockedMigratorScopeWithConfig(tb, ctx, nil, opts...)
}

func NewMockedMigratorScopeWithConfig(tb testing.TB, ctx context.Context, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (MigratorScope, Mocked) {
	tb.Helper()
	svcScp, mock := NewMockedServiceScopeWithConfig(tb, ctx, modifyConfig, opts...)

	d, err := newMigratorScope(ctx, svcScp, mock.TestConfig())
	require.NoError(tb, err)

	return d, mock
}

func newMigratorScope(_ context.Context, svcScp ServiceScope, _ config.Config) (v MigratorScope, err error) {
	d := &migratorScope{}

	d.ServiceScope = svcScp

	return d, nil
}
