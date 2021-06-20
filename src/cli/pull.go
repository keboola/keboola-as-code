package cli

import (
	"github.com/spf13/cobra"
	"keboola-as-code/src/diff"
	"keboola-as-code/src/model"
	"keboola-as-code/src/recipe"
	"keboola-as-code/src/state"
	"keboola-as-code/src/utils"
)

const pullShortDescription = `Pull configurations to the local project dir`
const pullLongDescription = `Command "pull"

Pull configurations from the Keboola Connection project.
Local files will be overwritten to match the project's state.

You can use the "--dry-run" flag to see
what needs to be done without modifying the files.
`

func pullCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "pull",
		Short: pullShortDescription,
		Long:  pullLongDescription,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			// Ask for the host/token, if not specified -> to make the first step easier
			root.options.AskUser(root.prompt, "Host")
			root.options.AskUser(root.prompt, "ApiToken")

			// Validate options
			if err := root.ValidateOptions([]string{"projectDirectory", "ApiHost", "ApiToken"}); err != nil {
				return err
			}

			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			// Validate token and get API
			api, err := root.GetStorageApi()
			if err != nil {
				return err
			}

			// Load manifest
			projectDir := root.options.ProjectDir()
			metadataDir := root.options.MetadataDir()
			manifest, err := model.LoadManifest(projectDir, metadataDir)
			if err != nil {
				return err
			}

			// Load project remote and local state
			state, err := state.LoadState(manifest, root.logger, root.ctx, api)
			if err != nil {
				return err
			}

			// Diff
			differ := diff.NewDiffer(state)
			diffResults, err := differ.Diff()
			if err != nil {
				return err
			}

			// Pull
			pull := recipe.Pull(diffResults).Log(root.logger)
			if err := pull.Invoke(root.ctx, manifest, root.api, root.logger); err != nil {
				return err
			}

			// Save manifest
			if err = manifest.Save(); err != nil {
				return err
			}
			root.logger.Debugf("Saved manifest file \"%s\".", utils.RelPath(projectDir, manifest.Path))

			// Done
			root.logger.Info("Pull done.")
			return nil
		},
	}

	// Pull command flags
	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")

	return cmd
}
