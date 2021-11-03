package cli

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
)

const (
	validateShortDescription = `Validate the local project directory`
	validateLongDescription  = `Command "validate"

Validate existence and contents of all files in the local project dir.
For components with a JSON schema, the content must match the schema.
`
)

func validateCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: validateShortDescription,
		Long:  validateLongDescription,
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
			projectManifest, err := manifest.LoadManifest(root.fs)
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
			projectState, ok := state.LoadState(stateOptions)
			if ok {
				logger.Debugf("Project local state has been successfully loaded.")
			} else if projectState.LocalErrors().Len() > 0 {
				return utils.PrefixError("project local state is invalid", projectState.LocalErrors())
			}

			// Validate schemas and encryption
			if err := Validate(projectState, logger, false); err != nil {
				return err
			}

			logger.Info("Everything is good.")
			return nil
		},
	}

	return cmd
}
