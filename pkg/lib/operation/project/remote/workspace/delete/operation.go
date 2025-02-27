package delete_op

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	Telemetry() telemetry.Telemetry
	Logger() log.Logger
	KeboolaProjectAPI() *keboola.AuthorizedAPI
}

func Run(ctx context.Context, d dependencies, branchID keboola.BranchID, workspace *keboola.WorkspaceWithConfig) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.workspace.delete")
	defer span.End(&err)

	logger := d.Logger()

	ctx, cancel := context.WithTimeoutCause(ctx, 10*time.Minute, errors.New("workspace deletion timeout"))
	defer cancel()

	logger.Infof(ctx, `Deleting the workspace "%s" (%s), please wait.`, workspace.Config.Name, workspace.Config.ID)
	err = d.KeboolaProjectAPI().DeleteWorkspace(
		ctx,
		branchID,
		workspace.Config.ID,
		workspace.Workspace.ID,
	)
	if err != nil {
		return err
	}
	logger.Infof(ctx, "Delete done.")

	return nil
}
