package env

import (
	"context"
	"fmt"
	"strings"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/sandboxesapi"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
)

type GenerateEnvOptions struct {
	TargetName string
	Workspace  *sandboxesapi.Sandbox
}

type dependencies interface {
	Fs() filesystem.Fs
	Logger() log.Logger
	SandboxesApiClient() client.Sender
	Tracer() trace.Tracer
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
}

func Run(ctx context.Context, opts GenerateEnvOptions, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.dbt.generate.env")
	defer telemetry.EndSpan(span, &err)

	// Check that we are in dbt directory
	if _, _, err := d.LocalDbtProject(ctx); err != nil {
		return err
	}

	workspace, err := sandboxesapi.GetRequest(opts.Workspace.ID).Send(ctx, d.SandboxesApiClient())
	if err != nil {
		return err
	}

	targetUpper := strings.ToUpper(opts.TargetName)
	d.Logger().Infof(`Commands to set environment for the dbt target:`)
	d.Logger().Infof(`  export DBT_KBC_%s_TYPE=%s`, targetUpper, workspace.Type)
	d.Logger().Infof(`  export DBT_KBC_%s_SCHEMA=%s`, targetUpper, workspace.Details.Connection.Schema)
	d.Logger().Infof(`  export DBT_%s_WAREHOUSE=%s`, targetUpper, workspace.Details.Connection.Warehouse)
	d.Logger().Infof(`  export DBT_%s_DATABASE=%s`, targetUpper, workspace.Details.Connection.Database)
	host := workspace.Details.Connection.Database
	if workspace.Type == sandboxesapi.TypeSnowflake {
		host = strings.Replace(host, ".snowflakecomputing.com", "", 1)
	}
	d.Logger().Infof(`  export DBT_%s_ACCOUNT=%s`, targetUpper, host)
	d.Logger().Infof(`  export DBT_%s_USER=%s`, targetUpper, workspace.User)
	d.Logger().Infof(`  export DBT_%s_PASSWORD=%s`, targetUpper, workspace.Password)
	return nil
}
