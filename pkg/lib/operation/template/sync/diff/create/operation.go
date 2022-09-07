package create

import (
	"context"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	Objects model.ObjectStates
}

type dependencies interface {
	Tracer() trace.Tracer
}

func Run(ctx context.Context, o Options, d dependencies) (results *diff.Results, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.template.sync.diff.create")
	defer telemetry.EndSpan(span, &err)

	differ := diff.NewDiffer(o.Objects)
	results, err = differ.Diff()
	if err != nil {
		return nil, err
	}
	return results, nil
}
