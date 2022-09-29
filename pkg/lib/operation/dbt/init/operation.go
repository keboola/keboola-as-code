package init

import (
	"context"
	"fmt"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/profile"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/sources"
)

type DbtInitOptions struct {
	TargetName    string
	WorkspaceName string
}

type dependencies interface {
	Fs() filesystem.Fs
	JobsQueueApiClient() client.Sender
	Logger() log.Logger
	SandboxesApiClient() client.Sender
	StorageApiClient() client.Sender
	Tracer() trace.Tracer
}

func Run(ctx context.Context, opts DbtInitOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.dbt.init")
	defer telemetry.EndSpan(span, &err)

	// Check that we are in dbt directory
	if !d.Fs().Exists(`dbt_project.yml`) {
		return fmt.Errorf(`missing file "dbt_project.yml" in the current directory`)
	}

	branch, err := storageapi.GetDefaultBranchRequest().Send(ctx, d.StorageApiClient())
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	d.Logger().Info(`Creating new workspace, please wait.`)
	// Create workspace
	config, err := sandboxesapi.Create(
		ctx,
		d.StorageApiClient(),
		d.JobsQueueApiClient(),
		branch.ID,
		opts.WorkspaceName,
		sandboxesapi.TypeSnowflake,
	)
	if err != nil {
		return fmt.Errorf("cannot create workspace: %w", err)
	}
	d.Logger().Infof(`Created new workspace "%s".`, opts.WorkspaceName)

	id, err := sandboxesapi.GetSandboxID(config)
	if err != nil {
		return fmt.Errorf("workspace config is invalid: %w", err)
	}

	workspace, err := sandboxesapi.GetRequest(id).Send(ctx, d.SandboxesApiClient())
	if err != nil {
		return fmt.Errorf("could not retrieve new workspace: %w", err)
	}

	// Generate profile
	err = profile.Run(ctx, opts.TargetName, d)
	if err != nil {
		return fmt.Errorf("could not generate profile: %w", err)
	}

	// Generate sources
	err = sources.Run(ctx, opts.TargetName, d)
	if err != nil {
		return fmt.Errorf("could not generate sources: %w", err)
	}

	// Generate env
	err = env.Run(ctx, env.GenerateEnvOptions{
		TargetName: opts.TargetName,
		Workspace:  workspace,
	}, d)
	if err != nil {
		return fmt.Errorf("could not generate env: %w", err)
	}

	return nil
}
