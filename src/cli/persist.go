package cli

import (
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"

	"github.com/spf13/cobra"
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
			stateOptions := state.NewOptions(projectManifest, api, root.ctx, logger)
			stateOptions.LoadLocalState = true
			stateOptions.SkipNotFoundErr = true
			projectState, ok := state.LoadState(stateOptions)
			if ok {
				logger.Debugf("Project local state has been successfully loaded.")
			} else {
				if projectState.LocalErrors().Len() > 0 {
					return utils.PrefixError("project local state is invalid", projectState.LocalErrors())
				}
			}

			// Persist new/untracked objects
			if newPersisted, err := projectState.PersistNew(); err == nil {
				if len(newPersisted) > 0 {
					logger.Info("New objects:")
					for _, object := range newPersisted {
						logger.Infof("\t+ %s %s %s", object.Kind().Abbr, object.ObjectId(), object.RelativePath())
					}
				}
			} else {
				return utils.PrefixError("cannot persist new objects", err)
			}

			// Persist deleted objets
			if deleted, err := projectState.PersistDeleted(); err == nil {
				if len(deleted) > 0 {
					logger.Info("Deleted objects:")
					for _, object := range deleted {
						logger.Infof("\t- %s %s", object.Kind().Abbr, object.RelativePath())
					}
				}
			} else {
				return utils.PrefixError("cannot persist deleted objects", err)
			}

			// Print remaining untracked paths
			projectState.LogUntrackedPaths(root.logger)

			// Normalize paths
			if err := Rename(projectState, logger, false, false); err != nil {
				return err
			}

			// Save manifest
			if changed, err := SaveManifest(projectManifest, logger); err != nil {
				return err
			} else if !changed {
				logger.Info(`Nothing to do.`)
			}

			logger.Info(`Persist done.`)
			return nil
		},
	}

	return cmd
}
