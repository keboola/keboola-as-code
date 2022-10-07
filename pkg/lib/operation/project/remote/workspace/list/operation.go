package list

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	StorageApiClient() client.Sender
	SandboxesApiClient() client.Sender
}

func Run(ctx context.Context, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.list")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, d.StorageApiClient())
	if err != nil {
		return fmt.Errorf("cannot find default branch: %w", err)
	}

	logger.Info("Loading workspaces, please wait.")
	sandboxes, err := sandboxesapi.List(ctx, d.StorageApiClient(), d.SandboxesApiClient(), branch.ID)
	if err != nil {
		return err
	}

	logger.Info("Found workspaces:")
	for _, sandbox := range sandboxes {
		logger.Infof("  %s", sandbox.String())
	}

	return nil
}
