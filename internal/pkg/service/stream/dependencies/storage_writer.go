package dependencies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter"
)

// storageWriterScope implements StorageWriterScope interface.
type storageWriterScope struct {
	StorageScope
	dependencies.DistributionScope
	volumes *diskwriter.Volumes
}

func NewStorageWriterScope(ctx context.Context, storageScp StorageScope, distScp dependencies.DistributionScope, cfg config.Config) (v StorageWriterScope, err error) {
	return newStorageWriterScope(ctx, storageScp, distScp, cfg)
}

func newStorageWriterScope(ctx context.Context, storageScp StorageScope, distScp dependencies.DistributionScope, cfg config.Config) (v StorageWriterScope, err error) {
	d := &storageWriterScope{}

	d.StorageScope = storageScp

	d.DistributionScope = distScp

	d.volumes, err = diskwriter.OpenVolumes(ctx, d, cfg.Storage.VolumesPath, cfg.Storage.Level.Local.Writer)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func NewMockedStorageWriterScope(tb testing.TB, ctx context.Context, opts ...dependencies.MockedOption) (StorageWriterScope, Mocked) {
	tb.Helper()
	return NewMockedStorageWriterScopeWithConfig(tb, ctx, nil, opts...)
}

func NewMockedStorageWriterScopeWithConfig(tb testing.TB, ctx context.Context, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (StorageWriterScope, Mocked) {
	tb.Helper()

	storageScp, mock := NewMockedStorageScopeWithConfig(tb, ctx, modifyConfig, opts...)

	distScp := dependencies.NewDistributionScope(mock.TestConfig().NodeID, mock.TestConfig().Distribution, storageScp)

	d, err := newStorageWriterScope(ctx, storageScp, distScp, mock.TestConfig())
	require.NoError(tb, err)

	return d, mock
}

func (s *storageWriterScope) Volumes() *diskwriter.Volumes {
	return s.volumes
}
