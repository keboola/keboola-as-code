package plugin

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Plugins) RegisterSinkPipelineOpener(fn pipeline.Opener) {
	p.sinkPipelineOpeners = append(p.sinkPipelineOpeners, fn)
}

func (p *Plugins) OpenSinkPipeline(ctx context.Context, sinkKey key.SinkKey, sinkType definition.SinkType, onClose func(ctx context.Context, cause string)) (pipeline.Pipeline, error) {
	for _, fn := range p.sinkPipelineOpeners {
		p, err := fn(ctx, sinkKey, sinkType, onClose)
		if errors.As(err, &pipeline.NoOpenerFoundError{}) {
			continue
		}

		if err != nil {
			return nil, err
		}

		return p, nil
	}
	return nil, pipeline.NoOpenerFoundError{SinkType: sinkType}
}
