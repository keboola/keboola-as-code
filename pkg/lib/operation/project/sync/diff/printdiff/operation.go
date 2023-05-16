package printdiff

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
)

type Options struct {
	PrintDetails      bool
	LogUntrackedPaths bool
}

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, projectState *project.State, o Options, d dependencies) (results *diff.Results, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.sync.diff.print")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Diff
	results, err = createDiff.Run(ctx, createDiff.Options{Objects: projectState}, d)
	if err != nil {
		return nil, err
	}

	// Log untracked paths
	if o.LogUntrackedPaths {
		projectState.LogUntrackedPaths(logger)
	}

	if results.Equal {
		logger.Info("No difference.")
	} else {
		// Explain
		logger.Info(diff.ChangeMark + " changed")
		logger.Info(diff.OnlyInRemoteMark + " remote state")
		logger.Info(diff.OnlyInLocalMark + " local state")
		logger.Info("")

		// Print diff
		logger.Info("Diff:")
		for _, line := range results.Format(o.PrintDetails) {
			logger.Info(line)
		}
	}

	return results, nil
}
