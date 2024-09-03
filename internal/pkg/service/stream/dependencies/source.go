package dependencies

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	sinkRouter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/router"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	storageRouter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	statsCollector "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/collector"
)

// sourceScope implements SourceScope interface.
type sourceScope struct {
	ServiceScope
	dependencies.DistributionScope
	encodingManager   *encoding.Manager
	connectionManager *connection.Manager
	sinkRouter        *sinkRouter.Router
	storageRouter     *storageRouter.Router
}

func (v *sourceScope) EncodingManager() *encoding.Manager {
	return v.encodingManager
}

func (v *sourceScope) ConnectionManager() *connection.Manager {
	return v.connectionManager
}

func (v *sourceScope) SinkRouter() *sinkRouter.Router {
	return v.sinkRouter
}

func (v *sourceScope) StorageRouter() *storageRouter.Router {
	return v.storageRouter
}

func NewSourceScope(serviceScp ServiceScope, distScp dependencies.DistributionScope, sourceType string, cfg config.Config) (v SourceScope, err error) {
	return newSourceScope(serviceScp, distScp, sourceType, cfg)
}

func NewMockedSourceScope(tb testing.TB, ctx context.Context, opts ...dependencies.MockedOption) (SourceScope, Mocked) {
	tb.Helper()
	return NewMockedSourceScopeWithConfig(tb, ctx, nil, opts...)
}

func NewMockedSourceScopeWithConfig(tb testing.TB, ctx context.Context, modifyConfig func(*config.Config), opts ...dependencies.MockedOption) (SourceScope, Mocked) {
	tb.Helper()

	svcScp, mock := NewMockedServiceScopeWithConfig(tb, ctx, modifyConfig, opts...)

	distScp := dependencies.NewDistributionScope(mock.TestConfig().NodeID, mock.TestConfig().Distribution, svcScp)

	d, err := newSourceScope(svcScp, distScp, "test-source", mock.TestConfig())
	require.NoError(tb, err)
	return d, mock
}

func newSourceScope(svcScp ServiceScope, distScp dependencies.DistributionScope, sourceType string, cfg config.Config) (v SourceScope, err error) {
	d := &sourceScope{}

	d.ServiceScope = svcScp

	d.DistributionScope = distScp

	d.connectionManager, err = connection.NewManager(d, cfg.Storage.Level.Local.Writer.Network, cfg.NodeID)
	if err != nil {
		return nil, err
	}

	d.encodingManager = encoding.NewManager(d)

	statsCollector.Start(d, d.encodingManager.Events(), cfg.Storage.Statistics.Collector, cfg.NodeID)

	d.sinkRouter, err = sinkRouter.New(d)
	if err != nil {
		return nil, err
	}

	d.storageRouter, err = storageRouter.New(d, cfg.NodeID, sourceType, cfg.Storage.Level.Local.Writer.Network)
	if err != nil {
		return nil, err
	}

	d.Plugins().RegisterSinkPipelineOpener(func(ctx context.Context, sinkKey key.SinkKey, sinkType definition.SinkType, onClose func(ctx context.Context, cause string)) (pipeline.Pipeline, error) {
		if d.Plugins().IsSinkWithLocalStorage(sinkType) {
			return d.storageRouter.OpenPipeline(ctx, sinkKey, onClose)
		}
		return nil, pipeline.NoOpenerFoundError{SinkType: sinkType}
	})

	return d, nil
}
