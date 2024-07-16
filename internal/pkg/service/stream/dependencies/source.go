package dependencies

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	sinkRouter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/router"
	storageRouter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router"
)

// sourceScope implements SourceScope interface.
type sourceScope struct {
	sourceParentScopes
	sinkRouter    sinkRouter.Router
	storageRouter *storageRouter.Router
}

type sourceParentScopes interface {
	ServiceScope
}

type sourceParentScopesImpl struct {
	ServiceScope
}

func (v *sourceScope) SinkRouter() sinkRouter.Router {
	return v.sinkRouter
}

func NewSourceScope(d sourceParentScopes, cfg config.Config) (v SourceScope, err error) {
	return newSourceScope(d, cfg)
}

func newSourceScope(parentScp sourceParentScopes, cfg config.Config) (v SourceScope, err error) {
	d := &sourceScope{}

	d.sourceParentScopes = parentScp

	d.sinkRouter, err = sinkRouter.New(d)
	if err != nil {
		return nil, err
	}

	d.storageRouter, err = storageRouter.New(d, cfg.Storage.Level.Local.Writer.Network, cfg.NodeID)
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
