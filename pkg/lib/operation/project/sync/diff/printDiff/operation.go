package printDiff

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/diff"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	createDiff "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/diff/create"
)

type Options struct {
	PrintDetails      bool
	LogUntrackedPaths bool
}

type dependencies interface {
	Logger() log.Logger
}

func Run(projectState *project.State, o Options, d dependencies) (*diff.Results, error) {
	logger := d.Logger()

	// Diff
	results, err := createDiff.Run(createDiff.Options{Objects: projectState})
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
		logger.Info(diff.OnlyInAMark + " remote state")
		logger.Info(diff.OnlyInBMark + " local state")
		logger.Info("")

		// Print diff
		logger.Info("Diff:")
		for _, line := range format(results, o.PrintDetails) {
			logger.Info(line)
		}
	}

	return results, nil
}

func format(r *diff.Results, details bool) []string {
	var out []string
	for _, result := range r.Results {
		if result.State != diff.ResultEqual {
			// Get path by key
			path := "" // todo

			// Message
			msg := fmt.Sprintf("%s %s %s", result.Mark(), result.Kind().Abbr, path)
			if !details && !result.ChangedFields.IsEmpty() {
				msg += " | changed: " + result.ChangedFields.String()
			}
			out = append(out, msg)

			// Changed fields
			if details {
				for _, field := range result.ChangedFields.All() {
					out = append(out, fmt.Sprintf("  %s:", field.Name()))
					for _, line := range strings.Split(field.Diff(), "\n") {
						out = append(out, fmt.Sprintf("  %s", line))
					}
				}
			}
		}
	}
	return out
}
