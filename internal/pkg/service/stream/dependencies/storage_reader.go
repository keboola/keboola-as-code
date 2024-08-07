package dependencies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskreader"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/cache"
)

// storageReaderScope implements StorageReaderScope interface.
type storageReaderScope struct {
	storageReaderParentScopes
	volumes           *diskreader.Volumes
	statisticsL1Cache *cache.L1
	statisticsL2Cache *cache.L2
}

type storageReaderParentScopes interface {
	StorageScope
}

type storageReaderParentScopesImpl struct {
	StorageScope
}

func NewStorageReaderScope(ctx context.Context, d storageReaderParentScopes, cfg config.Config) (v StorageReaderScope, err error) {
	return newStorageReaderScope(ctx, d, cfg)
}

func newStorageReaderScope(ctx context.Context, parentScp storageReaderParentScopes, cfg config.Config) (v StorageReaderScope, err error) {
	d := &storageReaderScope{}

	d.storageReaderParentScopes = parentScp

	d.volumes, err = diskreader.OpenVolumes(ctx, d, cfg.Storage.VolumesPath, cfg.Storage.Level.Local.Reader)
	if err != nil {
		return nil, err
	}

	d.statisticsL1Cache, err = cache.NewL1Cache(d)
	if err != nil {
		return nil, err
	}

	d.statisticsL2Cache, err = cache.NewL2Cache(d, d.statisticsL1Cache, cfg.Storage.Statistics.Cache.L2)
	if err != nil {
		return nil, err
	}

	return d, nil
}

func NewMockedStorageReaderScope(t *testing.T, opts ...dependencies.MockedOption) (StorageReaderScope, Mocked) {
	t.Helper()
	return NewMockedStorageReaderScopeWithConfig(t, nil, opts...)
}

func NewMockedStorageReaderScopeWithConfig(tb testing.TB, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (StorageReaderScope, Mocked) {
	tb.Helper()
	storageScp, mock := NewMockedStorageScopeWithConfig(tb, modifyConfig, opts...)
	d, err := newStorageReaderScope(mock.TestContext(), storageReaderParentScopesImpl{
		StorageScope: storageScp,
	}, mock.TestConfig())
	require.NoError(tb, err)
	return d, mock
}

func (s *storageReaderScope) Volumes() *diskreader.Volumes {
	return s.volumes
}

func (s *storageReaderScope) StatisticsL1Cache() *cache.L1 {
	return s.statisticsL1Cache
}

func (s *storageReaderScope) StatisticsL2Cache() *cache.L2 {
	return s.statisticsL2Cache
}
