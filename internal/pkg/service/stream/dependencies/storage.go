package dependencies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
)

// storageScope implements StorageScope interface.
type storageScope struct {
	ServiceScope
}

func NewStorageScope(ctx context.Context, svcScp ServiceScope, cfg config.Config) (v StorageScope, err error) {
	return newStorageScope(ctx, svcScp, cfg)
}

func NewMockedStorageScope(tb testing.TB, ctx context.Context, opts ...dependencies.MockedOption) (StorageScope, Mocked) {
	tb.Helper()
	return NewMockedStorageScopeWithConfig(tb, ctx, nil, opts...)
}

func NewMockedStorageScopeWithConfig(tb testing.TB, ctx context.Context, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (StorageScope, Mocked) {
	tb.Helper()

	svcScp, mock := NewMockedServiceScopeWithConfig(tb, ctx, modifyConfig, opts...)

	d, err := newStorageScope(ctx, svcScp, mock.TestConfig())
	require.NoError(tb, err)

	return d, mock
}

func newStorageScope(_ context.Context, svcScp ServiceScope, _ config.Config) (v StorageScope, err error) {
	d := &storageScope{}

	d.ServiceScope = svcScp

	return d, nil
}
