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
	Type keboola.SandboxWorkspaceType
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

	logger.Info(ctx, `Creating a new workspace, please wait.`)

	if keboola.SandboxWorkspaceSupportsSizes(o.Type) {
		// Python/R workspace
		opts := make([]keboola.CreateSandboxWorkspaceOption, 0)
		if len(o.Size) > 0 {
			opts = append(opts, keboola.WithSize(o.Size))
		}
		w, err := d.KeboolaProjectAPI().CreateSandboxWorkspace(ctx, branch.ID, o.Name, o.Type, opts...)
		if err != nil {
			return errors.Errorf("cannot create workspace: %w", err)
		}
		logger.Infof(ctx, `Created the new workspace "%s" (%s).`, o.Name, w.Config.ID)
		logger.Infof(ctx, "Credentials:\n  Host: %s\n  Password: %s", w.SandboxWorkspace.Host, w.SandboxWorkspace.Password)
	} else {
		// SQL workspace (Snowflake/BigQuery) — backend determined by project config
		session, err := d.KeboolaProjectAPI().CreateEditorSession(ctx, branch.ID, o.Name)
		if err != nil {
			return errors.Errorf("cannot create workspace: %w", err)
		}
		logger.Infof(ctx, `Created the new workspace "%s" (%s).`, o.Name, session.Config.ID)
	}

	return nil
}
