package delete_op

import (
	"context"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	wsinfo "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace"
)

type dependencies interface {
	Telemetry() telemetry.Telemetry
	Logger() log.Logger
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

// Run deletes a workspace. For Python/R workspaces it runs a delete queue job then removes the config.
// For SQL workspaces it calls DeleteEditorSession.
func Run(ctx context.Context, d dependencies, branchID keboola.BranchID, workspace *wsinfo.WorkspaceWithConfig) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.workspace.delete")
	defer span.End(&err)

	logger := d.Logger()

	ctx, cancel := context.WithTimeoutCause(ctx, 10*time.Minute, errors.New("workspace deletion timeout"))
	defer cancel()

	logger.Infof(ctx, `Deleting the workspace "%s" (%s), please wait.`, workspace.Config.Name, workspace.Config.ID)

	if workspace.App != nil {
		// Python/R workspace: delete via DataScience sandbox endpoint, then delete the config.
		err = deletePyRWorkspace(ctx, d.KeboolaProjectAPI(), branchID, workspace.Config.ID, workspace.App.ID)
	} else {
		// SQL workspace (Snowflake/BigQuery) — Session.ID is the EditorSessionID.
		err = d.KeboolaProjectAPI().DeleteEditorSession(ctx, branchID, workspace.Config.ID, workspace.Session.ID)
	}
	if err != nil {
		return err
	}

	logger.Infof(ctx, "Delete done.")
	return nil
}

// deletePyRWorkspace runs the sandbox delete queue job then removes the config.
func deletePyRWorkspace(ctx context.Context, api *keboola.AuthorizedAPI, branchID keboola.BranchID, configID keboola.ConfigID, appID keboola.DataScienceAppID) error {
	if _, err := api.DeleteSandboxWorkspaceJobRequest(keboola.SandboxWorkspaceID(appID)).Send(ctx); err != nil {
		return err
	}
	_, err := api.DeleteSandboxWorkspaceConfigRequest(branchID, configID).Send(ctx)
	return err
}
