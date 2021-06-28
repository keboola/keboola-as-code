package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

const validateShortDescription = `Validate the local project dir`
const validateLongDescription = `Command "validate"

Validate existence and contents of all files in the local project dir.
For components with a JSON schema, the content must match the schema.
`

func validateCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: validateShortDescription,
		Long:  validateLongDescription,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			root.options.AskUser(root.prompt, "Host")
			root.options.AskUser(root.prompt, "ApiToken")
			if err := root.ValidateOptions([]string{"projectDirectory", "ApiHost", "ApiToken"}); err != nil {
				return err
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger := root.logger

			// Validate token and get API
			api, err := root.GetStorageApi()
			if err != nil {
				return err
			}

			// Load manifest
			projectDir := root.options.ProjectDir()
			metadataDir := root.options.MetadataDir()
			projectManifest, err := manifest.LoadManifest(projectDir, metadataDir)
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
			errors := &utils.Error{}
			for _, config := range projectState.Configs() {
				component := projectState.GetComponent(*config.ComponentKey())
				if err := component.ValidateConfig(config.Local); err != nil {
					errors.Add(fmt.Errorf("config \"%s\" doesn't match schema: %s", config.ConfigFilePath(), err))
				}
			}
			for _, row := range projectState.ConfigRows() {
				component := projectState.GetComponent(*row.ComponentKey())
				if err := component.ValidateConfigRow(row.Local); err != nil {
					errors.Add(fmt.Errorf("config row \"%s\" doesn't match schema: %s", row.ConfigFilePath(), err))
				}
			}
			if errors.Len() > 0 {
				return errors
			}

			logger.Info("Everything is good.")
			return nil
		},
	}

	return cmd
}
