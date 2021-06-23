package cli

import (
	"fmt"
	"github.com/spf13/cobra"
	"keboola-as-code/src/diff"
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
			a, err := root.GetStorageApi()
			if err != nil {
				return err
			}

			// Load project remote and local state
			differ := diff.NewDiffer(root.ctx, a, root.logger, root.options.ProjectDir(), root.options.MetadataDir())
			if err := differ.LoadState(); err != nil {
				return err
			}

			return fmt.Errorf("TODO PULL")
		},
	}

	// Pull command flags
	cmd.Flags().SortFlags = true
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")

	return cmd
}
