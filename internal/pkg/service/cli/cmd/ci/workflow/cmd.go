package workflow

import (
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

type Flags struct {
	StorageAPIHost configmap.Value[string] `configKey:"storage-api-host" configShorthand:"H" configUsage:"storage API host, eg. \"connection.keboola.com\""`
	CI             configmap.Value[bool]   `configKey:"ci" configUsage:"generate workflows"`
	CIValidate     configmap.Value[bool]   `configKey:"ci-validate" configUsage:"create workflow to validate all branches on change"`
	CIPull         configmap.Value[bool]   `configKey:"ci-pull" configUsage:"create workflow to sync main branch each hour"`
	CIMainBranch   configmap.Value[string] `configKey:"ci-main-branch" configUsage:"name of the main branch for push/pull workflows"`
	CIPush         configmap.Value[bool]   `configKey:"ci-push" configUsage:"create workflow to push change in main branch to the project"`
}

func DefaultFlags() Flags {
	return Flags{
		CI:           configmap.NewValue(true),
		CIValidate:   configmap.NewValue(true),
		CIPush:       configmap.NewValue(true),
		CIPull:       configmap.NewValue(true),
		CIMainBranch: configmap.NewValue("main"),
	}
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workflows",
		Short: helpmsg.Read(`ci/workflows/short`),
		Long:  helpmsg.Read(`ci/workflows/long`),
		RunE: func(cmd *cobra.Command, args []string) (cmdErr error) {
			// Bind flags to struct
			f := Flags{}
			if err := p.BaseScope().ConfigBinder().Bind(cmd.Context(), cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Get dependencies
			d, err := p.LocalCommandScope(cmd.Context(), f.StorageAPIHost)
			if err != nil {
				return err
			}
			prj, _, err := d.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// Ask options
			options := AskWorkflowsOptions(f, d.Dialogs())

			// Generate workflows
			return workflowsGen.Run(cmd.Context(), prj.Fs(), options, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), DefaultFlags())

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
