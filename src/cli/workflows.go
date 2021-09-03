package cli

import (
	"github.com/spf13/cobra"

	"keboola-as-code/src/interaction"
	"keboola-as-code/src/workflows"
)

const workflowsShortDescription = `Generate Github Actions workflows`
const workflowsLongDescription = `Command "workflows"

Generate workflows for Github Actions:
- "validate" all branches on change.
- "push" - each change in the main branch will be pushed to the project.
- "pull" - main branch will be synchronized every 5 minutes.

You will be prompted which workflows you want to generate.

The secret KBC_STORAGE_API_TOKEN must be added to the GitHub repository.
`

func workflowsCommand(root *rootCommand) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflows",
		Short: workflowsShortDescription,
		Long:  workflowsLongDescription,
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			logger := root.logger
			prompt := root.prompt

			// Validate project directory
			if err := root.ValidateOptions([]string{"projectDirectory"}); err != nil {
				return err
			}

			// Default values
			options := &workflows.Options{
				Validate:   root.options.GetBool("ci-validate"),
				Push:       root.options.GetBool("ci-push"),
				Pull:       root.options.GetBool("ci-pull"),
				MainBranch: root.options.GetString("ci-main-branch"),
			}

			// Ask user if interactive terminal
			if prompt.Interactive {
				logger.Info("\nPlease confirm GitHub Actions you want to generate.")
				if !root.options.IsSet("ci-validate") {
					options.Validate = prompt.Confirm(&interaction.Confirm{
						Label:   "Generate \"validate\" workflow?\nAll GitHub branches will be validated on change.",
						Default: options.Validate,
					})
				}
				if !root.options.IsSet("ci-push") {
					options.Push = prompt.Confirm(&interaction.Confirm{
						Label:   "Generate \"push\" workflow?\nEach change in the main GitHub branch will be pushed to the project.",
						Default: options.Push,
					})
				}
				if !root.options.IsSet("ci-pull") {
					options.Pull = prompt.Confirm(&interaction.Confirm{
						Label:   "Generate \"pull\" workflow?\nThe main GitHub branch will be synchronized every 5 minutes.\nIf a change found, then a new commit is created and pushed.",
						Default: options.Pull,
					})
				}
				if !root.options.IsSet("ci-main-branch") && (options.Push || options.Pull) {
					if mainBranch, ok := prompt.Select(&interaction.Select{
						Label:   "Please select the main GitHub branch name:",
						Options: []string{"main", "master"},
						Default: options.MainBranch,
					}); ok {
						options.MainBranch = mainBranch
					}
				}
			}

			// Generate
			return workflows.GenerateFiles(root.logger, root.options.ProjectDir(), options)
		},
	}

	// Flags
	workflowsCmdFlags(cmd)

	return cmd
}

func workflowsCmdFlags(cmd *cobra.Command) {
	cmd.Flags().Bool("ci-validate", true, "create workflow to validate all branches on change")
	cmd.Flags().Bool("ci-push", true, "create workflow to push change in main branch to the project")
	cmd.Flags().Bool("ci-pull", true, "create workflow to sync main branch every 5 minutes")
	cmd.Flags().String("ci-main-branch", "main", "name of the main branch for push/pull workflows")
}
