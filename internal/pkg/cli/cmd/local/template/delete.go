package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	deleteOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/delete"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func DeleteCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `delete <instance>`,
		Short: helpmsg.Read(`local/template/delete/short`),
		Long:  helpmsg.Read(`local/template/delete/long`),
		RunE: func(cmd *cobra.Command, args []string) error {
			d := p.Dependencies()

			// Local project
			prj, err := d.LocalProject(false)
			if err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions())
			if err != nil {
				return err
			}

			// Select instance
			branchKey, instance, err := d.Dialogs().AskTemplateInstance(projectState, d.Options())
			if err != nil {
				return err
			}

			// Delete template
			options := deleteOp.Options{Branch: branchKey, Instance: instance.InstanceId, DryRun: d.Options().GetBool("dry-run")}
			return deleteOp.Run(projectState, options, d)
		},
	}

	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`instance`, "i", ``, "instance ID of the template to delete")
	cmd.Flags().Bool("dry-run", false, "print what needs to be done")
	return cmd
}
