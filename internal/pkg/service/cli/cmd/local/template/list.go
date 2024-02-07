package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/cliconfig"
	listOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/list"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func ListCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: helpmsg.Read(`local/template/list/short`),
		Long:  helpmsg.Read(`local/template/list/long`),
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

			branch, err := d.Dialogs().SelectBranch(projectState.LocalObjects().Branches(), `Select branch`)
			if err != nil {
				return err
			}

			branchState := projectState.MustGet(branch.BranchKey).(*model.BranchState)

			// List template instances
			return listOp.Run(cmd.Context(), branchState, d)
		},
	}

	listFlags := ListTemplateFlag{}
	_ = cliconfig.GenerateFlags(listFlags, cmd.Flags())

	return cmd
}
