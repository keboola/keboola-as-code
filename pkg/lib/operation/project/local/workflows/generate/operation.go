package generate

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/workflows"
)

type Options struct {
	Validate   bool // validate all branches
	Push       bool // push to the project on change in the main branch
	Pull       bool // periodical pull new changes to the main branch
	MainBranch string
}

func (o Options) Enabled() bool {
	return o.Validate || o.Push || o.Pull
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, fs filesystem.Fs, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.local.workflows.generate")
	defer telemetry.EndSpan(span, &err)

	if !o.Enabled() {
		return nil
	}

	return workflows.GenerateFiles(d.Logger(), fs, &workflows.Options{
		Validate:   o.Validate,
		Push:       o.Push,
		Pull:       o.Pull,
		MainBranch: o.MainBranch,
	})
}
