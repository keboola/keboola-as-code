package delete

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	deleteOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/delete"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flags struct {
	Branch   configmap.Value[string] `configKey:"branch" configShorthand:"b" configUsage:"branch ID or name"`
	Instance configmap.Value[string] `configKey:"instance" configShorthand:"i" configUsage:"instance ID of the template to delete"`
	DryRun   configmap.Value[bool]   `configKey:"dry-run" configUsage:"print what needs to be done"`
}

func Command(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `delete`,
		Short: helpmsg.Read(`local/template/delete/short`),
		Long:  helpmsg.Read(`local/template/delete/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			// Command must be used in project directory
			prj, d, err := p.LocalProject(cmd.Context(), false)
			if err != nil {
				return err
			}

			// flags
			f := Flags{}
			if err = configmap.Bind(utils.GetBindConfig(cmd.Flags(), args), &f); err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Select instance
			branchKey, instance, err := d.Dialogs().AskTemplateInstance(projectState, f.Branch, f.Instance)
			if err != nil {
				return err
			}

			// Delete template
			options := deleteOp.Options{Branch: branchKey, Instance: instance.InstanceID, DryRun: f.DryRun.Value}
			return deleteOp.Run(cmd.Context(), projectState, options, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flags{})

	return cmd
}
