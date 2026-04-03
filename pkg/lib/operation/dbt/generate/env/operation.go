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

// WorkspaceDetails holds the connection details for a workspace used in dbt env generation.
type WorkspaceDetails struct {
	Type      string // workspace type string, e.g. "snowflake", "python"
	Host      string
	User      string
	Password  string //nolint:gosec
	Database  string
	Schema    string
	Warehouse string
	// Fields for the keboola_snowflake dbt adapter (populated for SQL workspaces).
	// When both BaseURL and WorkspaceID are set, DBT_KBC_{TARGET}_BASE_URL / _BRANCH_ID / _WORKSPACE_ID are written.
	BaseURL     string // e.g. "https://query.keboola.com"
	BranchID    keboola.BranchID
	WorkspaceID string // numeric workspace ID from EditorSession
}

type Options struct {
	BranchKey  keboola.BranchKey
	TargetName string
	Workspace  WorkspaceDetails
	PrivateKey string               //nolint:gosec
	UseKeyPair bool                 // Whether key-pair authentication was requested (only add private key if true)
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

	targetUpper := strings.ToUpper(o.TargetName)
	host := o.Workspace.Host
	if o.Workspace.Type == "snowflake" {
		host = strings.Replace(host, ".snowflakecomputing.com", "", 1)
	}

	envVars := make(map[string]string)

	envVars[fmt.Sprintf("DBT_KBC_%s_TYPE", targetUpper)] = o.Workspace.Type
	envVars[fmt.Sprintf("DBT_KBC_%s_SCHEMA", targetUpper)] = o.Workspace.Schema
	envVars[fmt.Sprintf("DBT_KBC_%s_WAREHOUSE", targetUpper)] = o.Workspace.Warehouse
	envVars[fmt.Sprintf("DBT_KBC_%s_DATABASE", targetUpper)] = o.Workspace.Database

	linkedBucketEnvsMap := make(map[string]string)
	for _, bucket := range o.Buckets {
		if bucket.LinkedProjectID != 0 {
			envVarName := bucket.DatabaseEnv
			if _, exists := linkedBucketEnvsMap[envVarName]; !exists {
				stackPrefix, _, _ := strings.Cut(o.Workspace.Database, "_") // SAPI_..., KEBOOLA_..., etc.
				envVarValue := fmt.Sprintf("%s_%d", stackPrefix, bucket.LinkedProjectID)
				linkedBucketEnvsMap[envVarName] = envVarValue
				envVars[envVarName] = envVarValue
			}
		}
	}
	envVars[fmt.Sprintf("DBT_KBC_%s_ACCOUNT", targetUpper)] = host
	envVars[fmt.Sprintf("DBT_KBC_%s_USER", targetUpper)] = o.Workspace.User
	if o.UseKeyPair && len(o.PrivateKey) > 0 {
		envVars[fmt.Sprintf("DBT_KBC_%s_PRIVATE_KEY", targetUpper)] = o.PrivateKey
	}
	if len(o.Workspace.Password) > 0 {
		envVars[fmt.Sprintf("DBT_KBC_%s_PASSWORD", targetUpper)] = o.Workspace.Password
	}

	// Keboola adapter vars — written when the workspace was created via an editor session.
	if len(o.Workspace.BaseURL) > 0 && len(o.Workspace.WorkspaceID) > 0 {
		envVars[fmt.Sprintf("DBT_KBC_%s_BASE_URL", targetUpper)] = o.Workspace.BaseURL
		envVars[fmt.Sprintf("DBT_KBC_%s_BRANCH_ID", targetUpper)] = o.Workspace.BranchID.String()
		envVars[fmt.Sprintf("DBT_KBC_%s_WORKSPACE_ID", targetUpper)] = o.Workspace.WorkspaceID
	}

	// Sort keys for consistent output.
	keys := make([]string, 0, len(envVars))
	for k := range envVars {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var envContent strings.Builder
	for _, k := range keys {
		v := envVars[k]
		// Normalize multiline/special values for dotenv compatibility:
		// - Replace newlines and carriage returns by literal \n to keep a single line per var
		// - Escape existing double quotes
		// - Wrap in double quotes if any special characters present
		hasSpecial := strings.ContainsAny(v, " #\"'\\\n\r\t")
		if strings.Contains(v, "\n") || strings.Contains(v, "\r") {
			v = strings.ReplaceAll(v, "\r\n", "\n")
			v = strings.ReplaceAll(v, "\r", "\n")
			v = strings.ReplaceAll(v, "\n", `\\n`)
			hasSpecial = true
		}
		if hasSpecial {
			v = "\"" + strings.ReplaceAll(v, "\"", `\\"`) + "\""
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
