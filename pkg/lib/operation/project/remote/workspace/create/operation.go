package create

import (
	"context"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

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
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, o CreateOptions, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.workspace.create")
	defer span.End(&err)

	logger := d.Logger()

	branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeoutCause(ctx, 10*time.Minute, errors.New("workspace creation timeout"))
	defer cancel()

	opts := make([]keboola.CreateSandboxWorkspaceOption, 0)
	if len(o.Size) > 0 {
		opts = append(opts, keboola.WithSize(o.Size))
	}

	logger.Info(ctx, `Creating a new workspace, please wait.`)
	// Create workspace by API
	w, err := d.KeboolaProjectAPI().CreateSandboxWorkspace(
		ctx,
		branch.ID,
		o.Name,
		keboola.SandboxWorkspaceType(o.Type),
		opts...,
	)
	if err != nil {
		return errors.Errorf("cannot create workspace: %w", err)
	}

	workspace := w.SandboxWorkspace

	logger.Infof(ctx, `Created the new workspace "%s" (%s).`, o.Name, w.Config.ID)
	switch keboola.SandboxWorkspaceType(workspace.Type) {
	case keboola.SandboxWorkspaceTypeSnowflake:
		logger.Infof(
			ctx,
			"Credentials:\n  Host: %s\n  User: %s\n  Password: %s\n  Database: %s\n  Schema: %s\n  Warehouse: %s",
			workspace.Host,
			workspace.User,
			workspace.Password,
			workspace.Details.Connection.Database,
			workspace.Details.Connection.Schema,
			workspace.Details.Connection.Warehouse,
		)
	case keboola.SandboxWorkspaceTypeBigQuery:
		logger.Infof(
			ctx,
			"Credentials:\n  Host: %s\n  User: %s\n  Password: %s\n  Database: %s\n  Schema: %s",
			workspace.Host,
			workspace.User,
			workspace.Password,
			workspace.Details.Connection.Database,
			workspace.Details.Connection.Schema,
		)
	case keboola.SandboxWorkspaceTypePython:
		fallthrough
	case keboola.SandboxWorkspaceTypeR:
		logger.Infof(
			ctx,
			"Credentials:\n  Host: %s\n  Password: %s",
			workspace.Host,
			workspace.Password,
		)
	}

	return nil
}
