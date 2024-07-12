package plugin

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (p *Plugins) RegisterSinkPipelineOpener(fn pipeline.Opener) {
	p.sinkPipelineOpeners = append(p.sinkPipelineOpeners, fn)
}

func (p *Plugins) OpenSinkPipeline(ctx context.Context, sink definition.Sink) (pipeline.Pipeline, error) {
	for _, fn := range p.sinkPipelineOpeners {
		p, err := fn(ctx, sink)
		if errors.Is(err, definition.ErrCannotHandleSinkType) {
			continue
		}

		if err != nil {
			return nil, err
		}

		return p, nil
	}
	return nil, errors.Errorf(`no sink pipeline opener found for the sink type %q`, sink.Type)
}
