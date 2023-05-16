package init

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/profile"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/sources"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/listbuckets"
)

type DbtInitOptions struct {
	TargetName    string
	WorkspaceName string
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, o DbtInitOptions, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.dbt.init")
	defer telemetry.EndSpan(span, &err)

	// Check that we are in dbt directory
	if _, _, err := d.LocalDbtProject(ctx); err != nil {
		return err
	}

	branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	// Create workspace
	d.Logger().Info(`Creating a new workspace, please wait.`)
	w, err := d.KeboolaProjectAPI().CreateWorkspace(
		ctx,
		branch.ID,
		o.WorkspaceName,
		keboola.WorkspaceTypeSnowflake,
	)
	if err != nil {
		return errors.Errorf("cannot create workspace: %w", err)
	}
	d.Logger().Infof(`Created the new workspace "%s".`, o.WorkspaceName)
	workspace := w.Workspace

	// List buckets
	buckets, err := listbuckets.Run(ctx, listbuckets.Options{
		TargetName: o.TargetName,
	}, d)
	if err != nil {
		return errors.Errorf("could not list buckets: %w", err)
	}

	// Generate profile
	err = profile.Run(ctx, profile.Options{
		TargetName: o.TargetName,
	}, d)
	if err != nil {
		return errors.Errorf("could not generate profile: %w", err)
	}

	// Generate sources
	err = sources.Run(ctx, sources.Options{
		TargetName: o.TargetName,
		Buckets:    buckets,
	}, d)
	if err != nil {
		return errors.Errorf("could not generate sources: %w", err)
	}

	// Generate env
	err = env.Run(ctx, env.Options{
		TargetName: o.TargetName,
		Workspace:  workspace,
		Buckets:    buckets,
	}, d)
	if err != nil {
		return errors.Errorf("could not generate env: %w", err)
	}

	return nil
}
