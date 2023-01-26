package delete_op

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	KeboolaAPIClient() *keboola.API
}

func Run(ctx context.Context, d dependencies, branchID keboola.BranchID, workspace *keboola.WorkspaceWithConfig) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.delete")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	logger.Infof(`Deleting the workspace "%s" (%s), please wait.`, workspace.Config.Name, workspace.Config.ID)
	err = d.KeboolaAPIClient().DeleteWorkspace(
		ctx,
		branchID,
		workspace.Config.ID,
		workspace.Workspace.ID,
	)
	if err != nil {
		return err
	}
	logger.Infof("Delete done.")

	return nil
}
