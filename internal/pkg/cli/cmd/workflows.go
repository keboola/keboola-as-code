package cmd

import (
	"github.com/spf13/cobra"

	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/local/workflows/generate"
)

const (
	workflowsShortDescription = `Generate Github Actions workflows`
	workflowsLongDescription  = `Command "workflows"

Generate workflows for Github Actions:
- "validate" all branches on change.
- "push" - each change in the main branch will be pushed to the project.
- "pull" - main branch will be synchronized every 5 minutes.

You will be prompted which workflows you want to generate.

The secret KBC_STORAGE_API_TOKEN must be added to the GitHub repository.
`
)

func WorkflowsCommand(root *RootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflows",
		Short: workflowsShortDescription,
		Long:  workflowsLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d := root.Deps

			// Metadata directory is required
			d.LoadStorageApiHostFromManifest()
			if err := d.AssertMetaDirExists(); err != nil {
				return err
			}

			// Options
			options := d.Dialogs().AskWorkflowsOptions(d.Options())

			// Generate workflows
			return workflowsGen.Run(options, d)
		},
	}

	// Flags
	workflowsCmdFlags(cmd)

	return cmd
}

// workflowsCmdFlags are used also by init command.
func workflowsCmdFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("ci-validate", true, "create workflow to validate all branches on change")
	cmd.Flags().Bool("ci-push", true, "create workflow to push change in main branch to the project")
	cmd.Flags().Bool("ci-pull", true, "create workflow to sync main branch every 5 minutes")
	cmd.Flags().String("ci-main-branch", "main", "name of the main branch for push/pull workflows")
}
