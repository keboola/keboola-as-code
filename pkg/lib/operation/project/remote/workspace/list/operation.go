package list

import (
	"context"
	"sort"

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

func Run(ctx context.Context, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.workspace.list")
	defer span.End(&err)

	logger := d.Logger()

	branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return errors.Errorf("cannot find default branch: %w", err)
	}

	logger.Info(ctx, "Loading workspaces, please wait.")
	workspaces, err := d.KeboolaProjectAPI().ListSandboxWorkspaces(ctx, branch.ID)
	if err != nil {
		return err
	}
	sort.Slice(workspaces, func(i, j int) bool { return workspaces[i].Config.Name < workspaces[j].Config.Name })

	logger.Info(ctx, "Found workspaces:")
	for _, workspace := range workspaces {
		if keboola.SandboxWorkspaceSupportsSizes(workspace.SandboxWorkspace.Type) {
			logger.Infof(ctx, "  %s (ID: %s, Type: %s, Size: %s)", workspace.Config.Name, workspace.Config.ID, workspace.SandboxWorkspace.Type, workspace.SandboxWorkspace.Size)
		} else {
			logger.Infof(ctx, "  %s (ID: %s, Type: %s)", workspace.Config.Name, workspace.Config.ID, workspace.SandboxWorkspace.Type)
		}
	}

	return nil
}
