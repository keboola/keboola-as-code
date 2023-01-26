package detail

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type dependencies interface {
	KeboolaAPIClient() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

func Run(ctx context.Context, d dependencies, configID keboola.ConfigID) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.create")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	branch, err := d.KeboolaAPIClient().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	workspace, err := d.KeboolaAPIClient().GetWorkspace(ctx, branch.ID, configID)
	if err != nil {
		return err
	}

	c, w := workspace.Config, workspace.Workspace

	logger.Infof("Workspace \"%s\"\nID: %s\nType: %s", c.Name, c.ID, w.Type)
	if keboola.WorkspaceSupportsSizes(w.Type) {
		logger.Infof(`Size: %s`, w.Size)
	}

	switch w.Type {
	case keboola.WorkspaceTypeSnowflake:
		logger.Infof(
			"Credentials:\n  Host: %s\n  User: %s\n  Password: %s\n  Database: %s\n  Schema: %s\n  Warehouse: %s",
			w.Host,
			w.User,
			w.Password,
			w.Details.Connection.Database,
			w.Details.Connection.Schema,
			w.Details.Connection.Warehouse,
		)
	case keboola.WorkspaceTypePython:
		fallthrough
	case keboola.WorkspaceTypeR:
		logger.Infof(
			"Credentials:\n  Host: %s\n  Password: %s",
			w.Host,
			w.Password,
		)
	}

	return nil
}
