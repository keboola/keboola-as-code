package detail

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
}

func Run(ctx context.Context, d dependencies, configID keboola.WorkspaceConfigID) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.create")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	branch, err := keboola.GetDefaultBranchRequest().Send(ctx, d.KeboolaAPIClient())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	sandbox, err := keboola.GetWorkspace(ctx, d.KeboolaAPIClient(), d.SandboxesAPIClient(), branch.ID, configID)
	if err != nil {
		return err
	}

	c, s := sandbox.Config, sandbox.Sandbox

	logger.Infof("Workspace \"%s\"\nID: %s\nType: %s", c.Name, c.ID, s.Type)
	if keboola.WorkspaceSupportsSizes(s.Type) {
		logger.Infof(`Size: %s`, s.Size)
	}

	switch s.Type {
	case keboola.WorkspaceTypeSnowflake:
		logger.Infof(
			"Credentials:\n  Host: %s\n  User: %s\n  Password: %s\n  Database: %s\n  Schema: %s\n  Warehouse: %s",
			s.Host,
			s.User,
			s.Password,
			s.Details.Connection.Database,
			s.Details.Connection.Schema,
			s.Details.Connection.Warehouse,
		)
	case keboola.WorkspaceTypePython:
		fallthrough
	case keboola.WorkspaceTypeR:
		logger.Infof(
			"Credentials:\n  Host: %s\n  Password: %s",
			s.Host,
			s.Password,
		)
	}

	return nil
}
