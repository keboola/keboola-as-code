package init

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/init"
)

func AskDbtInit(f Flags, d *dialog.Dialogs) (initOp.DbtInitOptions, error) {
	targetName, err := utils.AskTargetName(d, f.TargetName)
	if err != nil {
		return initOp.DbtInitOptions{}, err
	}

	workspaceName, err := f.askWorkspaceNameForDbtInit(d)
	if err != nil {
		return initOp.DbtInitOptions{}, err
	}
	return initOp.DbtInitOptions{
		TargetName:    targetName,
		WorkspaceName: workspaceName,
	}, nil
}

func (f *Flags) askWorkspaceNameForDbtInit(d *dialog.Dialogs) (string, error) {
	if f.WorkspaceName.IsSet() {
		return f.WorkspaceName.Value, nil
	} else {
		name, ok := d.Ask(&prompt.Question{
			Label:     "Enter a name for a workspace to create",
			Validator: prompt.ValueRequired,
		})
		if !ok || len(name) == 0 {
			return "", errors.New("missing workspace name, please specify it")
		}

		return name, nil
	}
}
