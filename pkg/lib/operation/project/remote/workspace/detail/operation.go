package detail

import (
	"context"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/keboola/sandbox"
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

	// Fetch the sandbox config to determine workspace type.
	config, err := d.KeboolaProjectAPI().GetSandboxWorkspaceConfigRequest(branch.ID, configID).Send(ctx)
	if err != nil {
		return err
	}

	// Check if this is a Python/R workspace (has parameters.id) or a SQL editor session.
	_, wsIDErr := sandbox.GetSandboxWorkspaceID(config)
	if wsIDErr != nil {
		// SQL workspace — find the editor session linked to this config.
		sessions, e := d.KeboolaProjectAPI().ListEditorSessionsRequest().Send(ctx)
		if e != nil {
			return e
		}
		for _, s := range *sessions {
			if s.ConfigurationID == configID.String() {
				logger.Infof(ctx, "Workspace \"%s\"\nID: %s\nType: %s\nDatabase: %s\nSchema: %s",
					config.Name, configID, s.BackendType, s.WorkspaceDatabase, s.WorkspaceSchema)
				return nil
			}
		}
		return errors.Errorf(`no active editor session found for workspace "%s"`, configID)
	}

	// Python/R workspace
	workspace, err := sandbox.GetSandboxWorkspace(ctx, d.KeboolaProjectAPI(), branch.ID, configID)
	if err != nil {
		return err
	}

	c, w := workspace.Config, workspace.SandboxWorkspace

	logger.Infof(ctx, "Workspace \"%s\"\nID: %s\nType: %s", c.Name, c.ID, w.Type)
	if keboola.SandboxWorkspaceSupportsSizes(w.Type) {
		logger.Infof(ctx, `Size: %s`, w.Size)
	}

	if w.Host != "" || w.Password != "" {
		logger.Infof(ctx, "Credentials:\n  Host: %s\n  Password: %s", w.Host, w.Password)
	}

	return nil
}
