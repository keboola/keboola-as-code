package create

import (
	"context"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	workspace "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace"
)

type CreateOptions struct {
	Name string
	Type workspace.WorkspaceType
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

	if workspace.WorkspaceSupportsSizes(o.Type) {
		// Python/R workspace: create config then call DataScience sandbox endpoint.
		configID, err := createPyRWorkspace(ctx, d.KeboolaProjectAPI(), branch.ID, o.Name, o.Type, o.Size)
		if err != nil {
			return errors.Errorf("cannot create workspace: %w", err)
		}
		logger.Infof(ctx, `Created the new workspace "%s" (%s).`, o.Name, configID)
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

// createPyRWorkspace creates a Python/R workspace: creates the sandboxes config, then calls
// the DataScience sandbox service to provision the instance.
func createPyRWorkspace(ctx context.Context, api *keboola.AuthorizedAPI, branchID keboola.BranchID, name string, wsType workspace.WorkspaceType, size string) (keboola.ConfigID, error) {
	config, err := api.CreateSandboxWorkspaceConfigRequest(branchID, name).Send(ctx)
	if err != nil {
		return "", err
	}

	_, err = api.CreateDataScienceSandboxRequest(keboola.CreateDataScienceSandboxPayload{
		Type:            keboola.DataScienceAppType(wsType),
		ConfigurationID: string(config.ID),
		ComponentID:     string(keboola.SandboxWorkspacesComponent),
		BranchID:        branchID.String(),
		Size:            keboola.DataScienceSandboxSize(size),
	}).Send(ctx)
	if err != nil {
		return "", err
	}

	return config.ID, nil
}
