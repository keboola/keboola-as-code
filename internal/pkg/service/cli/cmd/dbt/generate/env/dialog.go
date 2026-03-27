package env

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	kenv "github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	genenv "github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
	wsinfo "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace"
)

func AskGenerateEnv(
	ctx context.Context,
	branchKey keboola.BranchKey,
	branchID keboola.BranchID,
	d *dialog.Dialogs,
	allWorkspaces []*wsinfo.WorkspaceWithConfig,
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

	if workspace.App != nil {
		// Python/R workspace — credential fields are not available from the listing.
		return genenv.Options{
			BranchKey:  branchKey,
			TargetName: targetName,
			Workspace: genenv.WorkspaceDetails{
				Type: workspace.Type(),
			},
			UseKeyPair: useKeyPair,
			PrivateKey: privateKey,
		}, nil
	}

	// SQL workspace (Snowflake/BigQuery) — fetch StorageWorkspace credentials via the editor session.
	if workspace.Session == nil {
		return genenv.Options{}, errors.Errorf(`no active editor session found for workspace %q`, workspace.Config.Name)
	}
	matchedSession := workspace.Session

	workspaceIDUint, err := strconv.ParseUint(matchedSession.WorkspaceID, 10, 64)
	if err != nil {
		return genenv.Options{}, errors.Errorf("cannot parse workspace ID %q: %w", matchedSession.WorkspaceID, err)
	}

	// StorageWorkspaceCreateCredentialsRequest creates new credentials on each call,
	// which rotates any previously issued credentials for this workspace.
	storageWS, err := api.StorageWorkspaceCreateCredentialsRequest(branchID, workspaceIDUint).Send(ctx)
	if err != nil {
		return genenv.Options{}, errors.Errorf("cannot fetch workspace credentials: %w", err)
	}

	deref := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}

	ws := genenv.WorkspaceDetails{
		Type:        keboola.SandboxWorkspaceType(matchedSession.BackendType),
		Host:        deref(storageWS.StorageWorkspaceDetails.Host),
		User:        deref(storageWS.StorageWorkspaceDetails.User),
		Database:    deref(storageWS.StorageWorkspaceDetails.Database),
		Schema:      deref(storageWS.StorageWorkspaceDetails.Schema),
		Warehouse:   deref(storageWS.StorageWorkspaceDetails.Warehouse),
		BranchID:    branchID,
		WorkspaceID: matchedSession.WorkspaceID,
	}

	// Use server-provided private key for SQL workspaces when available.
	if len(privateKey) == 0 && storageWS.StorageWorkspaceDetails.PrivateKey != nil && len(*storageWS.StorageWorkspaceDetails.PrivateKey) > 0 {
		privateKey = *storageWS.StorageWorkspaceDetails.PrivateKey
		useKeyPair = true
	}

	return genenv.Options{
		BranchKey:  branchKey,
		TargetName: targetName,
		Workspace:  ws,
		UseKeyPair: useKeyPair,
		PrivateKey: privateKey,
	}, nil
}
