package list

import (
	"context"
	"sort"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"golang.org/x/sync/errgroup"

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

func Run(ctx context.Context, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.remote.workspace.list")
	defer span.End(&err)

	logger := d.Logger()

	branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return errors.Errorf("cannot find default branch: %w", err)
	}

	logger.Info(ctx, "Loading workspaces, please wait.")

	// Fetch Python/R workspaces and editor sessions in parallel.
	// ListSandboxWorkspaces also returns all sandbox configs, so no separate fetch is needed.
	var pyRWorkspaces []*sandbox.SandboxWorkspaceWithConfig
	var allConfigs []*keboola.Config
	var sessions []*keboola.EditorSession

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		var e error
		pyRWorkspaces, allConfigs, e = sandbox.ListSandboxWorkspaces(grpCtx, d.KeboolaProjectAPI(), branch.ID)
		return e
	})
	grp.Go(func() error {
		result, e := d.KeboolaProjectAPI().ListEditorSessionsRequest().Send(grpCtx)
		if e != nil {
			return e
		}
		sessions = *result
		return nil
	})
	if err := grp.Wait(); err != nil {
		return err
	}

	// Build config name map for editor session name lookup.
	configNameMap := make(map[string]string)
	for _, c := range allConfigs {
		configNameMap[c.ID.String()] = c.Name
	}

	// Build combined list: Python/R workspaces + SQL editor sessions.
	all := make([]*sandbox.SandboxWorkspaceWithConfig, 0, len(pyRWorkspaces)+len(sessions))
	all = append(all, pyRWorkspaces...)
	for _, s := range sessions {
		name := configNameMap[s.ConfigurationID]
		all = append(all, &sandbox.SandboxWorkspaceWithConfig{
			Config: &keboola.Config{
				ConfigKey: keboola.ConfigKey{ID: keboola.ConfigID(s.ConfigurationID)},
				Name:      name,
			},
			SandboxWorkspace: &sandbox.SandboxWorkspace{
				Type: keboola.SandboxWorkspaceType(s.BackendType),
			},
		})
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Config.Name < all[j].Config.Name })

	logger.Info(ctx, "Found workspaces:")
	for _, workspace := range all {
		if keboola.SandboxWorkspaceSupportsSizes(workspace.SandboxWorkspace.Type) {
			logger.Infof(ctx, "  %s (ID: %s, Type: %s, Size: %s)", workspace.Config.Name, workspace.Config.ID, workspace.SandboxWorkspace.Type, workspace.SandboxWorkspace.Size)
		} else {
			logger.Infof(ctx, "  %s (ID: %s, Type: %s)", workspace.Config.Name, workspace.Config.ID, workspace.SandboxWorkspace.Type)
		}
	}

	return nil
}
