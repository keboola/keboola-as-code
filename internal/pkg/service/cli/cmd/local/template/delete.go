package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	deleteOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/delete"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type DeleteTemplateFlags struct {
	Branch   string `mapstructure:"branch" shorthand:"b" usage:"branch ID or name"`
	Instance string `mapstructure:"instance" shorthand:"i" usage:"instance ID of the template to delete"`
	DryRun   bool   `mapstructure:"dry-run" usage:"print what needs to be done"`
}

func DeleteCommand(p dependencies.Provider) *cobra.Command {
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

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			// Select instance
			branchKey, instance, err := d.Dialogs().AskTemplateInstance(projectState)
			if err != nil {
				return err
			}

			// Delete template
			options := deleteOp.Options{Branch: branchKey, Instance: instance.InstanceID, DryRun: d.Options().GetBool("dry-run")}
			return deleteOp.Run(cmd.Context(), projectState, options, d)
		},
	}

	cliconfig.MustGenerateFlags(DeleteTemplateFlags{}, cmd.Flags())

	return cmd
}
