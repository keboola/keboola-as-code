package workspace

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

type listDeps interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	StorageApiClient() client.Sender
	SandboxesApiClient() client.Sender
}

func List(ctx context.Context, d listDeps) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.list")
	defer telemetry.EndSpan(span, &err)

	w := d.Logger().InfoWriter()

	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, d.StorageApiClient())
	if err != nil {
		return fmt.Errorf("cannot find default branch: %w", err)
	}

	sandboxConfigs, err := sandboxesapi.ListConfigRequest(branch.ID).Send(ctx, d.StorageApiClient())
	if err != nil {
		return fmt.Errorf("cannot list workspace configs: %w", err)
	}

	sandboxes, err := sandboxesapi.ListRequest().Send(ctx, d.SandboxesApiClient())
	if err != nil {
		return fmt.Errorf("cannot list workspaces: %w", err)
	}
	sandboxesMap := make(map[string]*sandboxesapi.Sandbox, 0)
	for _, sandbox := range *sandboxes {
		sandboxesMap[sandbox.ID.String()] = sandbox
	}

	for _, sandboxConfig := range *sandboxConfigs {
		sandboxId, err := sandboxesapi.GetSandboxID(sandboxConfig)
		if err != nil {
			return fmt.Errorf("invalid workspace config: %w", err)
		}
		sandboxInstance := sandboxesMap[sandboxId.String()]

		w.Writef("ID: %s", sandboxInstance.ID)
		w.Writef("Name: %s", sandboxConfig.Name)
		w.Writef("Type: %s", sandboxInstance.Type)
		if sandboxesapi.SupportsSizes(sandboxInstance.Type) {
			w.Writef("Size: %s", sandboxInstance.Size)
		}
		w.Writef("")
	}

	return nil
}
