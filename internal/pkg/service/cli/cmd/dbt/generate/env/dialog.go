package env

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	kenv "github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	genenv "github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
)

func AskGenerateEnv(branchKey keboola.BranchKey, d *dialog.Dialogs, allWorkspaces []*keboola.SandboxWorkspaceWithConfig, f Flags, envs kenv.Provider) (genenv.Options, error) {
	targetName, err := d.AskTargetName(f.TargetName)
	if err != nil {
		return genenv.Options{}, err
	}

	workspace, err := d.AskWorkspace(allWorkspaces, f.WorkspaceID)
	if err != nil {
		return genenv.Options{}, err
	}

	// Check if private key is available in environment variables (for tests)
	// Look for TEST_SANDBOX_{workspace_name}_PRIVATE_KEY
	workspaceName := workspace.Config.Name
	// Normalize workspace name for environment variable lookup (uppercase, replace spaces/dashes with underscores)
	normalizedName := strings.ToUpper(strings.NewReplacer(" ", "_", "-", "_").Replace(workspaceName))
	privateKeyEnvVar := fmt.Sprintf("TEST_SANDBOX_%s_PRIVATE_KEY", normalizedName)
	privateKey := envs.Get(privateKeyEnvVar)

	useKeyPair := len(privateKey) > 0

	return genenv.Options{
		BranchKey:  branchKey,
		TargetName: targetName,
		Workspace:  workspace.SandboxWorkspace,
		UseKeyPair: useKeyPair,
		PrivateKey: privateKey,
	}, nil
}
