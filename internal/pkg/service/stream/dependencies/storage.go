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
	storageParentScopes
}

type storageParentScopes interface {
	ServiceScope
}

type storageParentScopesImpl struct {
	ServiceScope
}

func NewStorageScope(ctx context.Context, d storageParentScopes, cfg config.Config) (v StorageScope, err error) {
	return newStorageScope(ctx, d, cfg)
}

func NewMockedStorageScope(t *testing.T, opts ...dependencies.MockedOption) (StorageScope, Mocked) {
	t.Helper()
	return NewMockedStorageScopeWithConfig(t, nil, opts...)
}

func NewMockedStorageScopeWithConfig(tb testing.TB, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (StorageScope, Mocked) {
	tb.Helper()
	svcScp, mock := NewMockedServiceScopeWithConfig(tb, modifyConfig, opts...)
	d, err := newStorageScope(mock.TestContext(), storageParentScopesImpl{
		ServiceScope: svcScp,
	}, mock.TestConfig())
	require.NoError(tb, err)
	return d, mock
}

func newStorageScope(_ context.Context, parentScp storageParentScopes, _ config.Config) (v StorageScope, err error) {
	d := &storageScope{}

	d.storageParentScopes = parentScp

	return d, nil
}
