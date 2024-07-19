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
	storageWriterParentScopes
	volumes *diskwriter.Volumes
}

type storageWriterParentScopes interface {
	StorageScope
	dependencies.DistributionScope
}

type storageWriterParentScopesImpl struct {
	StorageScope
	dependencies.DistributionScope
}

func NewStorageWriterScope(ctx context.Context, storageScp StorageScope, distScp dependencies.DistributionScope, cfg config.Config) (v StorageWriterScope, err error) {
	return newStorageWriterScope(ctx, storageWriterParentScopesImpl{
		StorageScope:      storageScp,
		DistributionScope: distScp,
	}, cfg)
}

func newStorageWriterScope(ctx context.Context, parentScp storageWriterParentScopes, cfg config.Config) (v StorageWriterScope, err error) {
	d := &storageWriterScope{}

	d.storageWriterParentScopes = parentScp

	d.volumes, err = diskwriter.OpenVolumes(ctx, d, cfg.Storage.VolumesPath, cfg.Storage.Level.Local.Writer)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func NewMockedStorageWriterScope(t *testing.T, opts ...dependencies.MockedOption) (StorageWriterScope, Mocked) {
	t.Helper()
	return NewMockedStorageWriterScopeWithConfig(t, nil, opts...)
}

func NewMockedStorageWriterScopeWithConfig(tb testing.TB, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (StorageWriterScope, Mocked) {
	tb.Helper()
	storageScp, mock := NewMockedStorageScopeWithConfig(
		tb,
		modifyConfig,
		append([]dependencies.MockedOption{dependencies.WithEnabledDistribution()}, opts...)...,
	)
	d, err := newStorageWriterScope(mock.TestContext(), storageWriterParentScopesImpl{
		StorageScope:      storageScp,
		DistributionScope: mock,
	}, mock.TestConfig())
	require.NoError(tb, err)
	return d, mock
}

func (s *storageWriterScope) Volumes() *diskwriter.Volumes {
	return s.volumes
}
