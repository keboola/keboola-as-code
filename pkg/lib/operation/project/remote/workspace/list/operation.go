package list

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"go.opentelemetry.io/otel/trace"

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
		if sandboxesapi.SupportsSizes(sandbox.Sandbox.Type) {
			logger.Infof("  %s (ID: %s, Type: %s, Size: %s)", sandbox.Config.Name, sandbox.Config.ID, sandbox.Sandbox.Type, sandbox.Sandbox.Size)
		} else {
			logger.Infof("  %s (ID: %s, Type: %s)", sandbox.Config.Name, sandbox.Config.ID, sandbox.Sandbox.Type)
		}
	}

	return nil
}
