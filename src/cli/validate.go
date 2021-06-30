package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/schema"
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

			// Load manifest
			projectDir := root.options.ProjectDir()
			metadataDir := root.options.MetadataDir()
			projectManifest, err := manifest.LoadManifest(projectDir, metadataDir)
			if err != nil {
				return err
			}

			// Validate
			root.options.ApiHost = projectManifest.Project.ApiHost
			root.options.AskUser(root.prompt, "ApiToken")
			if err := root.ValidateOptions([]string{"projectDirectory", "ApiHost", "ApiToken"}); err != nil {
				return err
			}

			// Validate token and get API
			api, err := root.GetStorageApi()
			if err != nil {
				return err
			}

			// Load project local state
			projectState, ok := state.LoadState(projectManifest, logger, root.ctx, api, false)
			if ok {
				logger.Debugf("Project local state has been successfully loaded.")
			} else {
				if projectState.LocalErrors().Len() > 0 {
					return fmt.Errorf(
						"project local state is invalid:%s",
						projectState.LocalErrors(),
					)
				}
			}

			// Validate schemas
			if err := schema.ValidateSchemas(projectState); err != nil {
				return utils.WrapError("validation failed", err)
			} else {
				logger.Debug("Validation done.")
			}

			logger.Info("Everything is good.")
			return nil
		},
	}

	return cmd
}
