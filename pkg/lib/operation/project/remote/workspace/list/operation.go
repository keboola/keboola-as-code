package list

import (
	"context"
	"sort"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	wsinfo "github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/workspace"
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

	// Fetch Python/R workspaces (via DataScienceApp) and SQL editor sessions in parallel.
	var pyRWorkspaces []*wsinfo.WorkspaceWithConfig
	var allConfigs []*keboola.Config
	var sessions []*keboola.EditorSession

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		var e error
		pyRWorkspaces, allConfigs, e = ListPyRWorkspaces(grpCtx, d.KeboolaProjectAPI(), branch.ID)
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
	all := make([]*wsinfo.WorkspaceWithConfig, 0, len(pyRWorkspaces)+len(sessions))
	all = append(all, pyRWorkspaces...)
	for _, s := range sessions {
		name := configNameMap[s.ConfigurationID]
		all = append(all, &wsinfo.WorkspaceWithConfig{
			Config: &keboola.Config{
				ConfigKey: keboola.ConfigKey{ID: keboola.ConfigID(s.ConfigurationID)},
				Name:      name,
			},
			Session: s,
		})
	}


	sort.Slice(all, func(i, j int) bool { return all[i].Config.Name < all[j].Config.Name })

	logger.Info(ctx, "Found workspaces:")
	for _, workspace := range all {
		if workspace.SupportsSizes() {
			logger.Infof(ctx, "  %s (ID: %s, Type: %s, Size: %s)", workspace.Config.Name, workspace.Config.ID, workspace.Type(), workspace.Size())
		} else {
			logger.Infof(ctx, "  %s (ID: %s, Type: %s)", workspace.Config.Name, workspace.Config.ID, workspace.Type())
		}
	}

	return nil
}

// ListPyRWorkspaces fetches Python/R workspaces by listing DataScienceApps and their configs in parallel,
// joining by ConfigID. It also returns all sandbox configs so callers can look up SQL workspace names.
func ListPyRWorkspaces(ctx context.Context, api *keboola.AuthorizedAPI, branchID keboola.BranchID) ([]*wsinfo.WorkspaceWithConfig, []*keboola.Config, error) {
	var configs []*keboola.Config
	var apps []*keboola.DataScienceApp

	grp, grpCtx := errgroup.WithContext(ctx)
	grp.Go(func() error {
		data, e := api.ListSandboxWorkspaceConfigRequest(branchID).Send(grpCtx)
		if e != nil {
			return e
		}
		configs = *data
		return nil
	})
	grp.Go(func() error {
		data, e := api.ListDataScienceAppsRequest(
			keboola.WithDataScienceAppsComponentID(keboola.ComponentID(keboola.SandboxWorkspacesComponent)),
			keboola.WithDataScienceAppsBranchID(branchID.String()),
			keboola.WithDataScienceAppsType(keboola.DataScienceAppTypePython, keboola.DataScienceAppTypeR),
		).Send(grpCtx)
		if e != nil {
			return e
		}
		apps = *data
		return nil
	})
	if err := grp.Wait(); err != nil {
		return nil, nil, err
	}

	appsByConfigID := make(map[string]*keboola.DataScienceApp, len(apps))
	for _, app := range apps {
		appsByConfigID[app.ConfigID] = app
	}

	out := make([]*wsinfo.WorkspaceWithConfig, 0)
	for _, config := range configs {
		app, found := appsByConfigID[config.ID.String()]
		if !found {
			continue
		}
		out = append(out, &wsinfo.WorkspaceWithConfig{
			App:    app,
			Config: config,
		})
	}
	return out, configs, nil
}
