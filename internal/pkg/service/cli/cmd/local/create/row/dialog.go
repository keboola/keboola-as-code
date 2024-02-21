package row

import (
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	createRow "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/create/row"
)

func AskCreateRow(projectState *project.State, d *dialog.Dialogs, f Flags) (createRow.Options, error) {
	out := createRow.Options{}

	// Branch
	allBranches := projectState.LocalObjects().Branches()
	branch, err := d.SelectBranch(allBranches, `Select the target branch`, f.Branch)
	if err != nil {
		return out, err
	}
	out.BranchID = branch.ID

	// Config
	allConfigs := projectState.LocalObjects().ConfigsWithRowsFrom(branch.BranchKey)
	config, err := d.SelectConfig(allConfigs, `Select the target config`, f.Config)
	if err != nil {
		return out, err
	}
	out.ComponentID = config.ComponentID
	out.ConfigID = config.ID

	// Name
	name, err := d.AskObjectName(`config row`, f.Name)
	if err != nil {
		return out, err
	}
	out.Name = name

	return out, nil
}
