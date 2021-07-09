package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

const persistShortDescription = `Persist created and deleted configs/rows in manifest`
const persistLongDescription = `Command "persist"

This command writes the changes from the filesystem to the manifest.
- If you have created a new config/row, this command will write record to the manifest with a unique ID.
- If you have deleted a config/row, this command will delete record from the manifest.

No changes are made to the remote state of the project.

If you also want to change the remote state,
call the "push" command after the "persist" command.
`

func persistCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "persist",
		Short: persistShortDescription,
		Long:  persistLongDescription,
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

			// Persist untracked files
			if persisted, err := projectState.Persist(); err == nil {
				logger.Info("Persisted paths:")
				for _, path := range persisted {
					logger.Info(`- `, path)
				}
			} else {
				return utils.WrapError("cannot persist untracked files", err)
			}

			// Save manifest
			if projectManifest.IsChanged() {
				if err = projectManifest.Save(); err != nil {
					return err
				}
				root.logger.Debugf("Saved manifest file \"%s\".", utils.RelPath(projectManifest.ProjectDir, projectManifest.Path()))
			}

			return nil
		},
	}

	return cmd
}
