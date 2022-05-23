package template

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	listOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/list"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

func ListCommand(p dependencies.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: helpmsg.Read(`local/template/list/short`),
		Long:  helpmsg.Read(`local/template/list/long`),
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

			branch, err := d.Dialogs().SelectBranch(d.Options(), projectState.LocalObjects().Branches(), `Select branch`)
			if err != nil {
				return err
			}

			branchState := projectState.MustGet(branch.BranchKey).(*model.BranchState)

			// List template instances
			return listOp.Run(branchState, d)
		},
	}

	cmd.Flags().StringP(`branch`, "b", ``, "branch ID or name")
	return cmd
}
