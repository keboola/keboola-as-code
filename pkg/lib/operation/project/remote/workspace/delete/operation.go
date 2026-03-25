package delete_op

import (
	"context"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	Telemetry() telemetry.Telemetry
	Logger() log.Logger
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

// Run deletes a workspace. For Python/R workspaces, it uses the sandbox delete API.
// For SQL (Snowflake/BigQuery) workspaces created via editor sessions, workspace.SandboxWorkspace.ID
// holds the EditorSessionID and deletion goes through the editor session API.
func Run(ctx context.Context, d dependencies, branchID keboola.BranchID, workspace *keboola.SandboxWorkspaceWithConfig) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.workspace.delete")
	defer span.End(&err)

	logger := d.Logger()

	ctx, cancel := context.WithTimeoutCause(ctx, 10*time.Minute, errors.New("workspace deletion timeout"))
	defer cancel()

	logger.Infof(ctx, `Deleting the workspace "%s" (%s), please wait.`, workspace.Config.Name, workspace.Config.ID)

	if keboola.SandboxWorkspaceSupportsSizes(workspace.SandboxWorkspace.Type) {
		// Python/R workspace
		err = d.KeboolaProjectAPI().DeleteSandboxWorkspace(ctx, branchID, workspace.Config.ID, workspace.SandboxWorkspace.ID)
	} else {
		// SQL workspace (Snowflake/BigQuery) — SandboxWorkspace.ID stores the EditorSessionID
		err = d.KeboolaProjectAPI().DeleteEditorSession(ctx, branchID, workspace.Config.ID, keboola.EditorSessionID(workspace.SandboxWorkspace.ID))
	}
	if err != nil {
		return err
	}

	logger.Infof(ctx, "Delete done.")
	return nil
}
