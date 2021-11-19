package cli

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	fixPathsShortDescription = `Normalize all local paths`
	fixPathsLongDescription  = `Command "fix-paths"

Manifest file ".keboola/manifest.json" contains a naming for all local paths.

With this command you can rename all existing paths
to match the configured naming (eg. if the naming has been changed).
`
)

func fixPathsCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fix-paths",
		Short: fixPathsShortDescription,
		Long:  fixPathsLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger := root.logger

			// Validate project directory
			if err := ValidateMetadataFound(root.fs); err != nil {
				return err
			}

			// Validate token
			root.options.AskUser(root.prompt, "ApiToken")
			if err := root.ValidateOptions([]string{"ApiToken"}); err != nil {
				return err
			}

			// Load manifest
			projectManifest, err := manifest.LoadManifest(root.fs, root.logger)
			if err != nil {
				return err
			}

			// Validate token and get API
			root.options.ApiHost = projectManifest.Project.ApiHost
			api, err := root.GetStorageApi()
			if err != nil {
				return err
			}

			// Get Scheduler API
			schedulerApi, err := root.GetSchedulerApi()
			if err != nil {
				return err
			}

			// Load project local state
			stateOptions := state.NewOptions(projectManifest, api, schedulerApi, root.ctx, logger)
			stateOptions.LoadLocalState = true
			stateOptions.SkipNotFoundErr = true
			projectState, ok := state.LoadState(stateOptions)
			if ok {
				logger.Debugf("Project local state has been successfully loaded.")
			} else if projectState.LocalErrors().Len() > 0 {
				return utils.PrefixError("project local state is invalid", projectState.LocalErrors())
			}

			// Normalize paths
			dryRun := root.options.GetBool("dry-run")
			if err := Rename(root.ctx, projectState, logger, true, dryRun); err != nil {
				return err
			}

			// Print untracked paths
			projectState.LogUntrackedPaths(root.logger)

			// Save manifest
			if changed, err := SaveManifest(projectManifest, logger); err != nil {
				return err
			} else if !changed && !dryRun {
				logger.Info(`Nothing to do.`)
			}

			logger.Info(`Fix paths done.`)
			return nil
		},
	}

	// Flags
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")

	return cmd
}
