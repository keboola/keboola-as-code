package cli

import (
	"github.com/spf13/cobra"

	"keboola-as-code/src/manifest"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

const validateShortDescription = `Validate the local project directory`
const validateLongDescription = `Command "validate"

Validate existence and contents of all files in the local project dir.
For components with a JSON schema, the content must match the schema.
`

func validateCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: validateShortDescription,
		Long:  validateLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger := root.logger

			// Validate project directory
			if err := root.ValidateOptions([]string{"projectDirectory"}); err != nil {
				return err
			}

			// Validate token
			root.options.AskUser(root.prompt, "ApiToken")
			if err := root.ValidateOptions([]string{"ApiToken"}); err != nil {
				return err
			}

			// Load manifest
			projectDir := root.options.ProjectDir()
			metadataDir := root.options.MetadataDir()
			projectManifest, err := manifest.LoadManifest(projectDir, metadataDir)
			if err != nil {
				return err
			}

			// Validate token and get API
			root.options.ApiHost = projectManifest.Project.ApiHost
			api, err := root.GetStorageApi()
			if err != nil {
				return err
			}

			// Load project local state
			stateOptions := state.NewOptions(projectManifest, api, root.ctx, logger)
			stateOptions.LoadLocalState = true
			projectState, ok := state.LoadState(stateOptions)
			if ok {
				logger.Debugf("Project local state has been successfully loaded.")
			} else if projectState.LocalErrors().Len() > 0 {
				return utils.PrefixError("project local state is invalid", projectState.LocalErrors())
			}

			// Validate schemas and encryption
			if err := Validate(projectState, logger); err != nil {
				return err
			}

			logger.Info("Everything is good.")
			return nil
		},
	}

	return cmd
}
