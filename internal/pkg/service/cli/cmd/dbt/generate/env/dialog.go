package env

import (
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/cmd/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
)

func AskGenerateEnv(allWorkspaces []*keboola.WorkspaceWithConfig, d *dialog.Dialogs, f Flags) (env.Options, error) {
	targetName, err := utils.AskTargetName(d, f.TargetName)
	if err != nil {
		return env.Options{}, err
	}

	workspace, err := utils.AskWorkspace(allWorkspaces, d, f.WorkspaceID)
	if err != nil {
		return env.Options{}, err
	}

	return env.Options{
		TargetName: targetName,
		Workspace:  workspace.Workspace,
	}, nil
}
