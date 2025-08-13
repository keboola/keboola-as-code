package env

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/listbuckets"
)

type Options struct {
	BranchKey  keboola.BranchKey
	TargetName string
	Workspace  *keboola.SandboxWorkspace
	Buckets    []listbuckets.Bucket // optional, set if the buckets have been loaded in a parent command
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Fs() filesystem.Fs // Add filesystem dependency
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.dbt.generate.env")
	defer span.End(&err)

	// Check that we are in dbt directory
	// Get dbt project
	dbtProject, _, err := d.LocalDbtProject(ctx)
	if err != nil {
		return err
	}

	// List bucket, if not set
	o.Buckets, err = listbuckets.Run(ctx, listbuckets.Options{BranchKey: o.BranchKey, TargetName: o.TargetName}, d)
	if err != nil {
		return errors.Errorf("could not list buckets: %w", err)
	}

	// Load workspace credentials
	workspace, err := d.KeboolaProjectAPI().GetSandboxWorkspaceInstanceRequest(o.Workspace.ID).Send(ctx)
	if err != nil {
		return errors.Errorf("could not load workspace credentials: %w", err)
	}

	targetUpper := strings.ToUpper(o.TargetName)
	host := workspace.Host
	if workspace.Type == string(keboola.SandboxWorkspaceTypeSnowflake) {
		host = strings.Replace(host, ".snowflakecomputing.com", "", 1)
	}

	// Prepare content for .env.local
	var envContent strings.Builder
	envVars := make(map[string]string)

	envVars[fmt.Sprintf("DBT_KBC_%s_TYPE", targetUpper)] = workspace.Type
	envVars[fmt.Sprintf("DBT_KBC_%s_SCHEMA", targetUpper)] = workspace.Details.Connection.Schema
	envVars[fmt.Sprintf("DBT_KBC_%s_WAREHOUSE", targetUpper)] = workspace.Details.Connection.Warehouse
	envVars[fmt.Sprintf("DBT_KBC_%s_DATABASE", targetUpper)] = workspace.Details.Connection.Database

	linkedBucketEnvsMap := make(map[string]string) // Store env var name -> value
	for _, bucket := range o.Buckets {
		if bucket.LinkedProjectID != 0 {
			envVarName := bucket.DatabaseEnv
			if _, exists := linkedBucketEnvsMap[envVarName]; !exists {
				stackPrefix, _, _ := strings.Cut(workspace.Details.Connection.Database, "_") // SAPI_..., KEBOOLA_..., etc.
				envVarValue := fmt.Sprintf("%s_%d", stackPrefix, bucket.LinkedProjectID)
				linkedBucketEnvsMap[envVarName] = envVarValue
				envVars[envVarName] = envVarValue
			}
		}
	}
	envVars[fmt.Sprintf("DBT_KBC_%s_ACCOUNT", targetUpper)] = host
	envVars[fmt.Sprintf("DBT_KBC_%s_USER", targetUpper)] = workspace.User
	envVars[fmt.Sprintf("DBT_KBC_%s_PASSWORD", targetUpper)] = workspace.Password

	// Format KEY=VALUE pairs
	// Sort keys for consistent order
	keys := make([]string, 0, len(envVars))
	for k := range envVars {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := envVars[k]
		// Basic quoting for values containing spaces or special characters, though passwords might still be tricky
		// A more robust solution might involve dotenv-specific libraries if complex values are common.
		if strings.ContainsAny(v, " #\"'\\") {
			v = fmt.Sprintf(`\"%s\"`, strings.ReplaceAll(v, `\"`, `\\\"`))
		}
		_, _ = fmt.Fprintf(&envContent, "%s=%s\n", k, v)
	}

	// Write to .env.local
	envFilePath := filesystem.Join(dbtProject.Fs().WorkingDir(), ".env.local")
	envFile := filesystem.NewRawFile(envFilePath, envContent.String()).SetDescription("dbt environment variables")
	if err := d.Fs().WriteFile(ctx, envFile); err != nil {
		return errors.Errorf("cannot write file \"%s\": %w", envFilePath, err)
	}

	// Print info message
	l := d.Logger()
	l.Infof(ctx, `Environment variables for dbt target "%s" have been written to "%s".`, o.TargetName, envFilePath)
	l.Info(ctx, `To load the variables into your current shell session, run:`)
	l.Info(ctx, `  source .env.local`)
	l.Info(ctx, `Or use a tool like direnv.`)

	if len(linkedBucketEnvsMap) > 0 {
		var linkedBucketEnvs []string
		for envName := range linkedBucketEnvsMap {
			linkedBucketEnvs = append(linkedBucketEnvs, envName)
		}
		l.Info(ctx, "")
		l.Info(ctx, "Note:")
		l.Info(ctx, "  The project contains linked buckets that are shared from other projects.")
		l.Info(ctx, "  Each project has a different database, so additional environment variables")
		l.Infof(ctx, "  have been generated: \"%s\"", strings.Join(linkedBucketEnvs, `", "`))
	}

	return nil
}
