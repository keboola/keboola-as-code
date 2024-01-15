package env

import (
	"context"
	"strings"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/listbuckets"
)

type Options struct {
	TargetName string
	Workspace  *keboola.Workspace
	Buckets    []listbuckets.Bucket // optional, set if the buckets have been loaded in a parent command
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.API
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.dbt.generate.env")
	defer span.End(&err)

	// Check that we are in dbt directory
	if _, _, err := d.LocalDbtProject(ctx); err != nil {
		return err
	}

	// List bucket, if not set
	o.Buckets, err = listbuckets.Run(ctx, listbuckets.Options{TargetName: o.TargetName}, d)
	if err != nil {
		return errors.Errorf("could not list buckets: %w", err)
	}

	// Load workspace credentials
	workspace, err := d.KeboolaProjectAPI().GetWorkspaceInstanceRequest(o.Workspace.ID).Send(ctx)
	if err != nil {
		return errors.Errorf("could not load workspace credentials: %w", err)
	}

	targetUpper := strings.ToUpper(o.TargetName)
	host := workspace.Host
	if workspace.Type == keboola.WorkspaceTypeSnowflake {
		host = strings.Replace(host, ".snowflakecomputing.com", "", 1)
	}

	// Print ENVs
	l := d.Logger()
	l.InfofCtx(ctx, `Commands to set environment for the dbt target:`)
	l.InfofCtx(ctx, `  export DBT_KBC_%s_TYPE=%s`, targetUpper, workspace.Type)
	l.InfofCtx(ctx, `  export DBT_KBC_%s_SCHEMA=%s`, targetUpper, workspace.Details.Connection.Schema)
	l.InfofCtx(ctx, `  export DBT_KBC_%s_WAREHOUSE=%s`, targetUpper, workspace.Details.Connection.Warehouse)
	l.InfofCtx(ctx, `  export DBT_KBC_%s_DATABASE=%s`, targetUpper, workspace.Details.Connection.Database)

	linkedBucketEnvsMap := make(map[string]bool)
	for _, bucket := range o.Buckets {
		if bucket.LinkedProjectID != 0 && !linkedBucketEnvsMap[bucket.DatabaseEnv] {
			stackPrefix, _, _ := strings.Cut(workspace.Details.Connection.Database, "_") // SAPI_..., KEBOOLA_..., etc.
			linkedBucketEnvsMap[bucket.DatabaseEnv] = true                               // print only once
			l.InfofCtx(ctx, `  export %s=%s_%d`, bucket.DatabaseEnv, stackPrefix, bucket.LinkedProjectID)
		}
	}
	l.InfofCtx(ctx, `  export DBT_KBC_%s_ACCOUNT=%s`, targetUpper, host)
	l.InfofCtx(ctx, `  export DBT_KBC_%s_USER=%s`, targetUpper, workspace.User)
	l.InfofCtx(ctx, `  export DBT_KBC_%s_PASSWORD=%s`, targetUpper, workspace.Password)

	if len(linkedBucketEnvsMap) > 0 {
		var linkedBucketEnvs []string
		for env := range linkedBucketEnvsMap {
			linkedBucketEnvs = append(linkedBucketEnvs, env)
		}
		l.Info(ctx, "")
		l.Info(ctx, "Note:")
		l.Info(ctx, "  The project contains linked buckets that are shared from other projects.")
		l.Info(ctx, "  Each project has a different database, so additional environment variables")
		l.InfofCtx(ctx, "  have been generated: \"%s\"", strings.Join(linkedBucketEnvs, `", "`))
	}

	return nil
}
