package env

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	kenv "github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/keboola/sandbox"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	genenv "github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
)

func AskGenerateEnv(
	ctx context.Context,
	branchKey keboola.BranchKey,
	branchID keboola.BranchID,
	d *dialog.Dialogs,
	allWorkspaces []*sandbox.SandboxWorkspaceWithConfig,
	sessions []*keboola.EditorSession,
	f Flags,
	envs kenv.Provider,
	api *keboola.AuthorizedAPI,
) (genenv.Options, error) {
	targetName, err := d.AskTargetName(f.TargetName)
	if err != nil {
		return genenv.Options{}, err
	}

	workspace, err := d.AskWorkspace(allWorkspaces, f.WorkspaceID)
	if err != nil {
		return genenv.Options{}, err
	}

	// Check if private key is available in environment variables (for tests).
	// Look for TEST_SANDBOX_{workspace_name}_PRIVATE_KEY
	workspaceName := workspace.Config.Name
	normalizedName := strings.ToUpper(strings.NewReplacer(" ", "_", "-", "_").Replace(workspaceName))
	privateKeyEnvVar := fmt.Sprintf("TEST_SANDBOX_%s_PRIVATE_KEY", normalizedName)
	privateKey := envs.Get(privateKeyEnvVar)

	useKeyPair := len(privateKey) > 0

	if keboola.SandboxWorkspaceSupportsSizes(workspace.SandboxWorkspace.Type) {
		// Python/R workspace — use credentials directly.
		return genenv.Options{
			BranchKey:  branchKey,
			TargetName: targetName,
			Workspace:  workspace.SandboxWorkspace, // already *sandbox.SandboxWorkspace
			UseKeyPair: useKeyPair,
			PrivateKey: privateKey,
		}, nil
	}

	// SQL workspace (Snowflake/BigQuery) — fetch StorageWorkspace credentials via the editor session.
	// workspace.SandboxWorkspace.ID holds the EditorSessionID; find the matching session for WorkspaceID.
	var matchedSession *keboola.EditorSession
	for _, s := range sessions {
		if s.ConfigurationID == workspace.Config.ID.String() {
			matchedSession = s
			break
		}
	}
	if matchedSession == nil {
		return genenv.Options{}, errors.Errorf(`no active editor session found for workspace "%s"`, workspace.Config.ID)
	}

	workspaceIDUint, err := strconv.ParseUint(matchedSession.WorkspaceID, 10, 64)
	if err != nil {
		return genenv.Options{}, errors.Errorf("cannot parse workspace ID %q: %w", matchedSession.WorkspaceID, err)
	}

	storageWS, err := api.StorageWorkspaceCreateCredentialsRequest(branchID, workspaceIDUint).Send(ctx)
	if err != nil {
		return genenv.Options{}, errors.Errorf("cannot fetch workspace credentials: %w", err)
	}

	sandboxWS := sandboxWorkspaceFromStorage(storageWS, keboola.SandboxWorkspaceType(matchedSession.BackendType))

	// Use server-provided private key for SQL workspaces when available.
	if len(privateKey) == 0 && storageWS.StorageWorkspaceDetails.PrivateKey != nil && len(*storageWS.StorageWorkspaceDetails.PrivateKey) > 0 {
		privateKey = *storageWS.StorageWorkspaceDetails.PrivateKey
		useKeyPair = true
	}

	return genenv.Options{
		BranchKey:  branchKey,
		TargetName: targetName,
		Workspace:  sandboxWS,
		UseKeyPair: useKeyPair,
		PrivateKey: privateKey,
	}, nil
}

// sandboxWorkspaceFromStorage constructs a SandboxWorkspace from StorageWorkspace details.
func sandboxWorkspaceFromStorage(sw *keboola.StorageWorkspace, wsType keboola.SandboxWorkspaceType) *sandbox.SandboxWorkspace {
	deref := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	details := &sandbox.SandboxWorkspaceDetails{}
	details.Connection.Database = deref(sw.StorageWorkspaceDetails.Database)
	details.Connection.Schema = deref(sw.StorageWorkspaceDetails.Schema)
	details.Connection.Warehouse = deref(sw.StorageWorkspaceDetails.Warehouse)
	return &sandbox.SandboxWorkspace{
		Type:    wsType,
		Host:    deref(sw.StorageWorkspaceDetails.Host),
		User:    deref(sw.StorageWorkspaceDetails.User),
		Details: details,
	}
}
