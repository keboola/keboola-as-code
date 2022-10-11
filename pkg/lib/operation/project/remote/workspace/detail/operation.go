package detail

import (
	"context"
	"time"

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

func Run(ctx context.Context, d dependencies, configId sandboxesapi.ConfigID) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.create")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, d.StorageApiClient())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	sandbox, err := sandboxesapi.Get(ctx, d.StorageApiClient(), d.SandboxesApiClient(), branch.ID, configId)
	if err != nil {
		return err
	}

	c, s := sandbox.Config, sandbox.Sandbox

	logger.Infof("Workspace \"%s\"\nID: %s\nType: %s", c.Name, c.ID, s.Type)
	if sandboxesapi.SupportsSizes(s.Type) {
		logger.Infof(`Size: %s`, s.Size)
	}

	switch s.Type {
	case sandboxesapi.TypeSnowflake:
		logger.Infof(
			"Credentials:\n  Host: %s\n  User: %s\n  Password: %s\n  Database: %s\n  Schema: %s\n  Warehouse: %s",
			s.Host,
			s.User,
			s.Password,
			s.Details.Connection.Database,
			s.Details.Connection.Schema,
			s.Details.Connection.Warehouse,
		)
	case sandboxesapi.TypePython:
		fallthrough
	case sandboxesapi.TypeR:
		logger.Infof(
			"Credentials:\n  Host: %s\n  Password: %s",
			s.Host,
			s.Password,
		)
	}

	return nil
}
