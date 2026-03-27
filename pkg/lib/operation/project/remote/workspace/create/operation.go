package create

import (
	"context"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/keboola/keboola-sdk-go/v2/pkg/request"

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
		// Python/R workspace: create config, run queue job, then log config ID.
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

// createPyRWorkspace creates a Python/R workspace: creates the sandboxes config, runs the
// creation queue job, and returns the config ID. Credentials are not fetched here.
func createPyRWorkspace(ctx context.Context, api *keboola.AuthorizedAPI, branchID keboola.BranchID, name string, wsType keboola.SandboxWorkspaceType, size string) (keboola.ConfigID, error) {
	emptyConfig, err := api.CreateSandboxWorkspaceConfigRequest(branchID, name).Send(ctx)
	if err != nil {
		return "", err
	}

	params := map[string]any{
		"task":                 "create",
		"type":                 wsType,
		"shared":               false,
		"expirationAfterHours": uint64(0),
	}
	if len(size) > 0 {
		params["size"] = size
	}

	req := api.NewCreateJobRequest(keboola.SandboxWorkspacesComponent).
		WithConfig(emptyConfig.ID).
		WithConfigData(map[string]any{"parameters": params}).
		Build().
		WithOnSuccess(func(ctx context.Context, result *keboola.QueueJob) error {
			return api.WaitForQueueJob(ctx, result.ID)
		})
	if _, err = request.NewAPIRequest(request.NoResult{}, req).Send(ctx); err != nil {
		return "", err
	}

	return emptyConfig.ID, nil
}
