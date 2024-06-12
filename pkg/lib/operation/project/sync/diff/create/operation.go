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

func Run(ctx context.Context, o Options, d dependencies, allowTargetEnv bool) (results *diff.Results, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.sync.diff.create")
	defer span.End(&err)

	differ := diff.NewDiffer(o.Objects, allowTargetEnv)
	results, err = differ.Diff()
	if err != nil {
		return nil, err
	}
	return results, nil
}
