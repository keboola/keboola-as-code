package init

import (
	"context"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/profile"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/sources"
)

type DbtInitOptions struct {
	TargetName    string
	WorkspaceName string
}

type dependencies interface {
	JobsQueueAPIClient() client.Sender
	Logger() log.Logger
	Tracer() trace.Tracer
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
	SandboxesAPIClient() client.Sender
	KeboolaAPIClient() client.Sender
}

func Run(ctx context.Context, opts DbtInitOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.dbt.init")
	defer telemetry.EndSpan(span, &err)

	// Check that we are in dbt directory
	if _, _, err := d.LocalDbtProject(ctx); err != nil {
		return err
	}

	branch, err := keboola.GetDefaultBranchRequest().Send(ctx, d.KeboolaAPIClient())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	d.Logger().Info(`Creating a new workspace, please wait.`)
	// Create workspace
	s, err := keboola.Create(
		ctx,
		d.KeboolaAPIClient(),
		branch.ID,
		opts.WorkspaceName,
		keboola.WorkspaceTypeSnowflake,
	)
	if err != nil {
		return errors.Errorf("cannot create workspace: %w", err)
	}
	d.Logger().Infof(`Created the new workspace "%s".`, opts.WorkspaceName)

	workspace := s.Sandbox

	// Generate profile
	err = profile.Run(ctx, opts.TargetName, d)
	if err != nil {
		return errors.Errorf("could not generate profile: %w", err)
	}

	// Generate sources
	err = sources.Run(ctx, opts.TargetName, d)
	if err != nil {
		return errors.Errorf("could not generate sources: %w", err)
	}

	// Generate env
	err = env.Run(ctx, env.GenerateEnvOptions{
		TargetName: opts.TargetName,
		Workspace:  workspace,
	}, d)
	if err != nil {
		return errors.Errorf("could not generate env: %w", err)
	}

	return nil
}
