package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	renameOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/rename"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func RenameCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   `rename`,
		Short: helpmsg.Read(`local/template/rename/short`),
		Long:  helpmsg.Read(`local/template/rename/long`),
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

			// Ask
			renameOpts, err := d.Dialogs().AskRenameInstance(projectState)
			if err != nil {
				return err
			}

			// Rename template instance
			return renameOp.Run(cmd.Context(), projectState, renameOpts, d)
		},
	}

	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	cmd.Flags().StringP(`instance`, "i", ``, "instance ID of the template to delete")
	cmd.Flags().StringP(`new-name`, "n", ``, "new name of the template instance")
	return cmd
}
