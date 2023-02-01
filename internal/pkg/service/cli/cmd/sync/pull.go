package sync

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/pull"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func PullCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pull",
		Short: helpmsg.Read(`sync/pull/short`),
		Long:  helpmsg.Read(`sync/pull/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			publicDeps, err := p.DependenciesForLocalCommand()
			if err != nil {
				return err
			}

			logger := publicDeps.Logger()
			force := publicDeps.Options().GetBool(`force`)

			// Command must be used in project directory
			prj, _, err := publicDeps.LocalProject(force)
			if err != nil {
				if !force && errors.As(err, &project.InvalidManifestError{}) {
					logger.Info()
					logger.Info("Use --force to override the invalid local state.")
				}
				return err
			}

			// Authentication
			prjDeps, err := p.DependenciesForRemoteCommand()
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.PullOptions(force), prjDeps)
			if err != nil {
				if !force && errors.As(err, &loadState.InvalidLocalStateError{}) {
					logger.Info()
					logger.Info("Use --force to override the invalid local state.")
				}
				return err
			}

			// Options
			options := pull.Options{
				DryRun:            prjDeps.Options().GetBool(`dry-run`),
				LogUntrackedPaths: true,
			}

			// Send cmd successful/failed event
			defer prjDeps.EventSender().SendCmdEvent(prjDeps.CommandCtx(), time.Now(), &cmdErr, "sync-pull")

			// Pull
			return pull.Run(prjDeps.CommandCtx(), projectState, options, prjDeps)
		},
	}

	// Flags
	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("force", false, "ignore invalid local state")
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
