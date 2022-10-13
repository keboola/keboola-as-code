package branch

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type Options struct {
	Name string
	Pull bool
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
	StorageApiClient() client.Sender
}

func Run(ctx context.Context, o Options, d dependencies) (branch *storageapi.Branch, err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.create.branch")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	// Create branch by API
	branch = &storageapi.Branch{Name: o.Name}
	if _, err := storageapi.CreateBranchRequest(branch).Send(ctx, d.StorageApiClient()); err != nil {
		return nil, errors.Errorf(`cannot create branch: %w`, err)
	}

	logger.Info(fmt.Sprintf(`Created new branch "%s".`, branch.Name))
	return branch, nil
}
