package create

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/client"
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
	Tracer() trace.Tracer
	Logger() log.Logger
	KeboolaAPIClient() client.Sender
	JobsQueueAPIClient() client.Sender
	SandboxesAPIClient() client.Sender
}

func Run(ctx context.Context, o CreateOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.create")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	branch, err := keboola.GetDefaultBranchRequest().Send(ctx, d.KeboolaAPIClient())
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
	s, err := keboola.CreateWorkspace(
		ctx,
		d.KeboolaAPIClient(),
		branch.ID,
		o.Name,
		o.Type,
		opts...,
	)
	if err != nil {
		return errors.Errorf("cannot create workspace: %w", err)
	}

	sandbox := s.Sandbox

	logger.Infof(`Created the new workspace "%s" (%s).`, o.Name, s.Config.ID)
	switch sandbox.Type {
	case keboola.WorkspaceTypeSnowflake:
		logger.Infof(
			"Credentials:\n  Host: %s\n  User: %s\n  Password: %s\n  Database: %s\n  Schema: %s\n  Warehouse: %s",
			sandbox.Host,
			sandbox.User,
			sandbox.Password,
			sandbox.Details.Connection.Database,
			sandbox.Details.Connection.Schema,
			sandbox.Details.Connection.Warehouse,
		)
	case keboola.WorkspaceTypePython:
		fallthrough
	case keboola.WorkspaceTypeR:
		logger.Infof(
			"Credentials:\n  Host: %s\n  Password: %s",
			sandbox.Host,
			sandbox.Password,
		)
	}

	return nil
}
