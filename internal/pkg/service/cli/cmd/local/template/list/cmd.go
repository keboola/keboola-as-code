package list

import (
	"github.com/spf13/cobra"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/helpmsg"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	listOp "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/list"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Flag struct {
	Branch configmap.Value[string] `configKey:"branch" configShorthand:"b" configUsage:"branch ID or name"`
}

func Command(p dependencies.Provider) *cobra.Command {
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

			// flags
			f := Flag{}
			if err = p.BaseScope().ConfigBinder().Bind(cmd.Flags(), args, &f); err != nil {
				return err
			}

			// Load project state
			projectState, err := prj.LoadState(loadState.LocalOperationOptions(), d)
			if err != nil {
				return err
			}

			branch, err := d.Dialogs().SelectBranch(projectState.LocalObjects().Branches(), `Select branch`, f.Branch)
			if err != nil {
				return err
			}

			branchState := projectState.MustGet(branch.BranchKey).(*model.BranchState)

			// List template instances
			return listOp.Run(cmd.Context(), branchState, d)
		},
	}

	configmap.MustGenerateFlags(cmd.Flags(), Flag{})

	return cmd
}
