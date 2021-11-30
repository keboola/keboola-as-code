package sync

import (
	"errors"
	"time"

	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/sync/pull"
)

const (
	pullShortDescription = `Pull configurations to the project directory`
	pullLongDescription  = `Command "pull"

Pull configurations from the Keboola Connection project.
Local files will be overwritten to match the project's state.

You can use the "--dry-run" flag to see
what needs to be done without modifying the files.
`
)

func PullCommand(depsProvider dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pull",
		Short: pullShortDescription,
		Long:  pullLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			d := depsProvider.Dependencies()
			start := time.Now()
			logger := d.Logger()

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options := pull.Options{
				DryRun:            d.Options().GetBool(`dry-run`),
				Force:             d.Options().GetBool(`force`),
				LogUntrackedPaths: true,
			}

			// Send cmd successful/failed event
			if eventSender, err := d.EventSender(); err == nil {
				defer func() {
					eventSender.SendCmdEvent(start, cmdErr, "pull")
				}()
			} else {
				return err
			}

			// Pull
			if err := pull.Run(options, d); err != nil && errors.As(err, &loadState.InvalidLocalStateError{}) {
				logger.Info()
				logger.Info("Use --force to override the invalid local state.")
				return err
			} else if err != nil {
				return err
			}
			return nil
		},
	}

	// Flags
	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("force", false, "ignore invalid local state")
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
