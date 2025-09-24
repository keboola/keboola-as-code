package env

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
)

func AskGenerateEnv(branchKey keboola.BranchKey, d *dialog.Dialogs, allWorkspaces []*keboola.SandboxWorkspaceWithConfig, f Flags) (env.Options, error) {
	targetName, err := d.AskTargetName(f.TargetName)
	if err != nil {
		return env.Options{}, err
	}

	workspace, err := d.AskWorkspace(allWorkspaces, f.WorkspaceID)
	if err != nil {
		return env.Options{}, err
	}

	return env.Options{
		BranchKey:  branchKey,
		TargetName: targetName,
		Workspace:  workspace.SandboxWorkspace,
	}, nil
}
