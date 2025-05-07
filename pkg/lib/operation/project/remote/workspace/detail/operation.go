package detail

import (
	"context"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, d dependencies, configID keboola.ConfigID) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.workspace.create")
	defer span.End(&err)

	logger := d.Logger()

	branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeoutCause(ctx, 10*time.Minute, errors.New("workspace details timeout"))
	defer cancel()

	workspace, err := d.KeboolaProjectAPI().GetWorkspace(ctx, branch.ID, configID)
	if err != nil {
		return err
	}

	c, w := workspace.Config, workspace.Workspace

	logger.Infof(ctx, "Workspace \"%s\"\nID: %s\nType: %s", c.Name, c.ID, w.Type)
	if keboola.WorkspaceSupportsSizes(w.Type) {
		logger.Infof(ctx, `Size: %s`, w.Size)
	}

	switch w.Type {
	case keboola.WorkspaceTypeSnowflake:
		logger.Infof(
			ctx,
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
			ctx,
			"Credentials:\n  Host: %s\n  Password: %s",
			w.Host,
			w.Password,
		)
	}

	return nil
}
