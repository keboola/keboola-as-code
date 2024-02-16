package workflow

import (
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	workflowsGen "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
)

type Flags struct {
	CI           configmap.Value[bool]   `configKey:"ci" configUsage:"generate workflow"`
	CIValidate   configmap.Value[bool]   `configKey:"ci-validate" configUsage:"create workflow to validate all branches on change"`
	CIPull       configmap.Value[bool]   `configKey:"ci-pull" configUsage:"create workflow to sync main branch each hour"`
	CIMainBranch configmap.Value[string] `configKey:"ci-main-branch" configUsage:"name of the main branch for push/pull workflow"`
	CIPush       configmap.Value[bool]   `configKey:"ci-push" configUsage:"create workflow to push change in main branch to the project"`
}

func (f Flags) Normalize() {
	return
}

func (f Flags) Validate() error {
	return nil
}

func DefaultFlags() *Flags {
	return &Flags{
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
		RunE: func(cmd *cobra.Command, args []string) (err error) {

			flags := Flags{}
			err = configmap.Bind(configmap.BindConfig{
				Flags:     cmd.Flags(),
				Args:      args,
				EnvNaming: env.NewNamingConvention("KBC_"),
				Envs:      env.Empty(),
			}, &flags)
			if err != nil {
				return err
			}

			// Get dependencies
			d, err := p.LocalCommandScope(cmd.Context())
			if err != nil {
				return err
			}
			prj, _, err := d.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			options := AskWorkflowsOptions(flags, d.Dialogs())
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
