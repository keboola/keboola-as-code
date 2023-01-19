package ci

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

func WorkflowsCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflows",
		Short: helpmsg.Read(`ci/workflows/short`),
		Long:  helpmsg.Read(`ci/workflows/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			d, err := p.DependenciesForLocalCommand()
			if err != nil {
				return err
			}

			// Local project
			prj, _, err := d.LocalProject(false)
			if err != nil {
				return err
			}

			// Options
			options := d.Dialogs().AskWorkflowsOptions(d.Options())

			// Generate workflows
			return workflowsGen.Run(d.CommandCtx(), prj.Fs(), options, d)
		},
	}

	// Flags
	WorkflowsCmdFlags(cmd.Flags())

	return cmd
}

// WorkflowsCmdFlags are used also by init command.
func WorkflowsCmdFlags(flags *pflag.FlagSet) {
	flags.Bool("ci", true, "generate workflows")
	flags.Bool("ci-validate", true, "create workflow to validate all branches on change")
	flags.Bool("ci-push", true, "create workflow to push change in main branch to the project")
	flags.Bool("ci-pull", true, "create workflow to sync main branch each hour")
	flags.String("ci-main-branch", "main", "name of the main branch for push/pull workflows")
}
