package dbtinit

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/prompt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	initOp "github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/init"
)

func AskDbtInit(d *dialog.Dialogs, f Flags, branchKey keboola.BranchKey) (initOp.DbtInitOptions, error) {
	targetName, err := d.AskTargetName(f.TargetName)
	if err != nil {
		return initOp.DbtInitOptions{}, err
	}

	workspaceName, err := askWorkspaceNameForDbtInit(d, f)
	if err != nil {
		return initOp.DbtInitOptions{}, err
	}

	return initOp.DbtInitOptions{
		BranchKey:     branchKey,
		TargetName:    targetName,
		WorkspaceName: workspaceName,
	}, nil
}

func askWorkspaceNameForDbtInit(d *dialog.Dialogs, f Flags) (string, error) {
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
