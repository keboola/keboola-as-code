package ci

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

type WorkflowFlags struct {
	CI           bool   `mapstructure:"ci" usage:"generate workflows"`
	CIValidate   bool   `mapstructure:"ci-validate" usage:"create workflow to validate all branches on change"`
	CIPush       bool   `mapstructure:"ci-push" usage:"create workflow to push change in main branch to the project"`
	CIPull       bool   `mapstructure:"ci-pull" usage:"create workflow to sync main branch each hour"`
	CIMainBranch string `mapstructure:"ci-main-branch" usage:"name of the main branch for push/pull workflows"`
}

func DefaultWorkflowFlags() *WorkflowFlags {
	return &WorkflowFlags{
		CI:           true,
		CIValidate:   true,
		CIPush:       true,
		CIPull:       true,
		CIMainBranch: "main",
	}
}

func WorkflowsCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflows",
		Short: helpmsg.Read(`ci/workflows/short`),
		Long:  helpmsg.Read(`ci/workflows/long`),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			// Ask options
			options := p.BaseScope().Dialogs().AskWorkflowsOptions()

			// Get dependencies
			d, err := p.LocalCommandScope(cmd.Context())
			if err != nil {
				return err
			}
			prj, _, err := d.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// Generate workflows
			return workflowsGen.Run(cmd.Context(), prj.Fs(), options, d)
		},
	}

	cliconfig.MustGenerateFlags(DefaultWorkflowFlags(), cmd.Flags())

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
