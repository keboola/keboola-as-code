package create

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type Options struct {
	Objects model.ObjectStates
}

type dependencies interface {
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, o Options, d dependencies, opts ...diff.Option) (results *diff.Results, err error) {
	_, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.sync.diff.create")
	defer span.End(&err)

	differ := diff.NewDiffer(o.Objects, opts...)
	results, err = differ.Diff()
	if err != nil {
		return nil, err
	}
	return results, nil
}
