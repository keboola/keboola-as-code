package delete_op

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	KeboolaAPIClient() client.Sender
	SandboxesAPIClient() client.Sender
	JobsQueueAPIClient() client.Sender
}

func Run(ctx context.Context, d dependencies, branchID keboola.BranchID, sandbox *keboola.WorkspaceWithConfig) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.delete")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	logger.Infof(`Deleting workspace "%s" (%s), please wait.`, sandbox.Config.Name, sandbox.Config.ID)
	err = keboola.DeleteWorkspace(
		ctx,
		d.KeboolaAPIClient(),
		d.JobsQueueAPIClient(),
		branchID,
		sandbox.Config.ID,
		sandbox.Sandbox.ID,
	)
	if err != nil {
		return err
	}
	logger.Infof("Delete done.")

	return nil
}
