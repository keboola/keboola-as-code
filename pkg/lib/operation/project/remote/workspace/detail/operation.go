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

	// Fetch the sandbox config to determine workspace type.
	config, err := d.KeboolaProjectAPI().GetSandboxWorkspaceConfigRequest(branch.ID, configID).Send(ctx)
	if err != nil {
		return err
	}

	// Check if this is a Python/R workspace (has parameters.id) or a SQL editor session.
	wsID, found, err := config.Content.GetNested("parameters.id")
	if err != nil {
		return err
	}

	if !found {
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

	// Python/R workspace — fetch DataScienceApp for credentials.
	workspaceIDStr, ok := wsID.(string)
	if !ok {
		return errors.Errorf("config.parameters.id is not a string")
	}

	app, err := d.KeboolaProjectAPI().GetDataScienceAppRequest(keboola.DataScienceAppID(workspaceIDStr)).Send(ctx)
	if err != nil {
		return err
	}

	logger.Infof(ctx, "Workspace \"%s\"\nID: %s\nType: %s", config.Name, config.ID, app.Type)
	if keboola.SandboxWorkspaceSupportsSizes(keboola.SandboxWorkspaceType(app.Type)) {
		logger.Infof(ctx, `Size: %s`, app.Size)
	}
	if app.URL != "" {
		logger.Infof(ctx, "URL: %s", app.URL)
	}

	return nil
}
