package create

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type CreateOptions struct {
	Name string
	Type string
	Size string
}

type dependencies interface {
	KeboolaAPIClient() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

func Run(ctx context.Context, o CreateOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.create")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	branch, err := d.KeboolaAPIClient().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	opts := make([]keboola.CreateWorkspaceOption, 0)
	if len(o.Size) > 0 {
		opts = append(opts, keboola.WithSize(o.Size))
	}

	logger.Info(`Creating a new workspace, please wait.`)
	// Create workspace by API
	w, err := d.KeboolaAPIClient().CreateWorkspace(
		ctx,
		branch.ID,
		o.Name,
		o.Type,
		opts...,
	)
	if err != nil {
		return errors.Errorf("cannot create workspace: %w", err)
	}

	workspace := w.Workspace

	logger.Infof(`Created new workspace "%w" (%w).`, o.Name, w.Config.ID)
	switch workspace.Type {
	case keboola.WorkspaceTypeSnowflake:
		logger.Infof(
			"Credentials:\n  Host: %w\n  User: %w\n  Password: %w\n  Database: %w\n  Schema: %w\n  Warehouse: %w",
			workspace.Host,
			workspace.User,
			workspace.Password,
			workspace.Details.Connection.Database,
			workspace.Details.Connection.Schema,
			workspace.Details.Connection.Warehouse,
		)
	case keboola.WorkspaceTypePython:
		fallthrough
	case keboola.WorkspaceTypeR:
		logger.Infof(
			"Credentials:\n  Host: %w\n  Password: %w",
			workspace.Host,
			workspace.Password,
		)
	}

	return nil
}
