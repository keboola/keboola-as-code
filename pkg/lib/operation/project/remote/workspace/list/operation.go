package list

import (
	"context"
	"sort"

	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type dependencies interface {
	KeboolaAPIClient() *keboola.API
	Logger() log.Logger
	Tracer() trace.Tracer
}

func Run(ctx context.Context, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.project.remote.workspace.list")
	defer telemetry.EndSpan(span, &err)

	logger := d.Logger()

	branch, err := d.KeboolaAPIClient().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return errors.Errorf("cannot find default branch: %w", err)
	}

	logger.Info("Loading workspaces, please wait.")
	workspaces, err := d.KeboolaAPIClient().ListWorkspaces(ctx, branch.ID)
	if err != nil {
		return err
	}
	sort.Slice(workspaces, func(i, j int) bool { return workspaces[i].Config.Name < workspaces[j].Config.Name })

	logger.Info("Found workspaces:")
	for _, workspace := range workspaces {
		if keboola.WorkspaceSupportsSizes(workspace.Workspace.Type) {
			logger.Infof("  %s (ID: %s, Type: %s, Size: %s)", workspace.Config.Name, workspace.Config.ID, workspace.Workspace.Type, workspace.Workspace.Size)
		} else {
			logger.Infof("  %s (ID: %s, Type: %s)", workspace.Config.Name, workspace.Config.ID, workspace.Workspace.Type)
		}
	}

	return nil
}
