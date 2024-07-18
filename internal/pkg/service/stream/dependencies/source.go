package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	sinkRouter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/router"
	storageRouter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	statsCollector "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/statistics/collector"
)

// sourceScope implements SourceScope interface.
type sourceScope struct {
	sourceParentScopes
	encodingManager *encoding.Manager
	sinkRouter      *sinkRouter.Router
	storageRouter   *storageRouter.Router
}

type sourceParentScopes interface {
	dependencies.DistributionScope
	ServiceScope
}

type sourceParentScopesImpl struct {
	dependencies.DistributionScope
	ServiceScope
}

func (v *sourceScope) EncodingManager() *encoding.Manager {
	return v.encodingManager
}

func (v *sourceScope) SinkRouter() *sinkRouter.Router {
	return v.sinkRouter
}

func NewSourceScope(d ServiceScope, sourceType string, cfg config.Config) (v SourceScope, err error) {
	distScope := dependencies.NewDistributionScope(cfg.NodeID, cfg.Distribution, d)
	return newSourceScope(sourceParentScopesImpl{ServiceScope: d, DistributionScope: distScope}, sourceType, cfg)
}

func newSourceScope(parentScp sourceParentScopes, sourceType string, cfg config.Config) (v SourceScope, err error) {
	d := &sourceScope{}

	d.sourceParentScopes = parentScp

	d.encodingManager = encoding.NewManager(d)

	statsCollector.Start(d, d.encodingManager.Events(), cfg.Storage.Statistics.Collector, cfg.NodeID)

	d.sinkRouter, err = sinkRouter.New(d)
	if err != nil {
		return nil, err
	}

	d.storageRouter, err = storageRouter.New(d, sourceType, cfg.Storage.Level.Local.Writer.Network)
	if err != nil {
		return nil, err
	}

	d.Plugins().RegisterSinkPipelineOpener(func(ctx context.Context, sinkKey key.SinkKey, sinkType definition.SinkType) (pipeline.Pipeline, error) {
		if d.Plugins().IsSinkWithLocalStorage(sinkType) {
			return d.storageRouter.OpenPipeline(ctx, sinkKey)
		}
		return nil, pipeline.NoOpenerFoundError{SinkType: sinkType}
	})

	return d, nil
}
