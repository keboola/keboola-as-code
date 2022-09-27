package workspace

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/sandboxesapi"
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
	SandboxesApiClient() client.Sender
}

func Create(ctx context.Context, o CreateOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.create")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, d.StorageApiClient())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	opts := make([]sandboxesapi.Option, 0)
	if len(o.Size) > 0 {
		opts = append(opts, sandboxesapi.WithSize(o.Size))
	}

	logger.Info(`Creating new workspace, please wait.`)
	// Create workspace by API
	config, err := sandboxesapi.Create(
		ctx,
		d.StorageApiClient(),
		d.JobsQueueApiClient(),
		branch.ID,
		o.Name,
		o.Type,
		opts...,
	)
	if err != nil {
		return fmt.Errorf("cannot create workspace: %w", err)
	}

	id, err := sandboxesapi.GetSandboxID(config)
	if err != nil {
		return fmt.Errorf("workspace config is invalid: %w", err)
	}

	sandbox, err := sandboxesapi.GetRequest(id).Send(ctx, d.SandboxesApiClient())
	if err != nil {
		return fmt.Errorf("could not retrieve new workspace: %w", err)
	}

	logger.Infof(`Created new workspace "%s".`, o.Name)
	switch sandbox.Type {
	case sandboxesapi.TypeSnowflake:
		logger.Infof(
			"Credentials:\n  Host: %s\n  User: %s\n  Password: %s\n  Database: %s\n  Schema: %s\n  Warehouse: %s",
			sandbox.Host,
			sandbox.User,
			sandbox.Password,
			sandbox.Details.Connection.Database,
			sandbox.Details.Connection.Schema,
			sandbox.Details.Connection.Warehouse,
		)
	case sandboxesapi.TypePython:
		fallthrough
	case sandboxesapi.TypeR:
		logger.Infof(
			"Credentials:\n  host: %s\n  password: %s",
			sandbox.Host,
			sandbox.Password,
		)
	}

	return nil
}
