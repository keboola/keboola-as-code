package dependencies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
)

// storageReaderScope implements StorageReaderScope interface.
type storageReaderScope struct {
	StorageScope
	volumes *diskreader.Volumes
}

func NewStorageReaderScope(ctx context.Context, storageScp StorageScope, cfg config.Config) (v StorageReaderScope, err error) {
	return newStorageReaderScope(ctx, storageScp, cfg)
}

func newStorageReaderScope(ctx context.Context, storageScp StorageScope, cfg config.Config) (v StorageReaderScope, err error) {
	d := &storageReaderScope{}

	d.StorageScope = storageScp

	d.volumes, err = diskreader.OpenVolumes(ctx, d, cfg.Storage.VolumesPath, cfg.Storage.Level.Local.Reader)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func NewMockedStorageReaderScope(tb testing.TB, ctx context.Context, opts ...dependencies.MockedOption) (StorageReaderScope, Mocked) {
	tb.Helper()
	return NewMockedStorageReaderScopeWithConfig(tb, ctx, nil, opts...)
}

func NewMockedStorageReaderScopeWithConfig(tb testing.TB, ctx context.Context, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (StorageReaderScope, Mocked) {
	tb.Helper()

	storageScp, mock := NewMockedStorageScopeWithConfig(tb, ctx, modifyConfig, opts...)

	d, err := newStorageReaderScope(ctx, storageScp, mock.TestConfig())
	require.NoError(tb, err)

	return d, mock
}

func (s *storageReaderScope) Volumes() *diskreader.Volumes {
	return s.volumes
}
