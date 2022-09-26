package workspace

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/sandbox"
	"github.com/keboola/go-client/pkg/storageapi"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type CreateOptions struct {
	Name string
	Type string
	Size string
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	StorageApiClient() client.Sender
	JobsQueueApiClient() client.Sender
}

func Create(ctx context.Context, o CreateOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.create.branch")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, d.StorageApiClient())
	if err != nil {
		return err
	}

	// Create workspace by API
	if _, err := sandbox.Create(
		ctx,
		d.StorageApiClient(),
		d.JobsQueueApiClient(),
		branch.ID,
		o.Name,
		o.Type,
		sandbox.WithSize(o.Size),
	); err != nil {
		return fmt.Errorf("cannot create workspace: %w", err)
	}

	logger.Info(fmt.Sprintf(`Created new workspace "%s".`, o.Name))
	return nil
}
