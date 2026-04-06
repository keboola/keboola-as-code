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
	Buckets    []listbuckets.Bucket // optional, set if the buckets have been loaded in a parent command
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	StorageAPIToken() keboola.Token
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Fs() filesystem.Fs
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

	// setVar writes a key=value pair only when value is non-empty.
	// Python/R workspaces only populate Type; all Snowflake connection fields are absent.
	// Omitting empty vars keeps .env.local clean and avoids confusing placeholder lines.
	setVar := func(key, value string) {
		if value != "" {
			envVars[key] = value
		}
	}

	setVar(fmt.Sprintf("DBT_KBC_%s_TYPE", targetUpper), o.Workspace.Type)
	setVar(fmt.Sprintf("DBT_KBC_%s_SCHEMA", targetUpper), o.Workspace.Schema)
	setVar(fmt.Sprintf("DBT_KBC_%s_WAREHOUSE", targetUpper), o.Workspace.Warehouse)
	setVar(fmt.Sprintf("DBT_KBC_%s_DATABASE", targetUpper), o.Workspace.Database)

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
	setVar(fmt.Sprintf("DBT_KBC_%s_ACCOUNT", targetUpper), host)
	setVar(fmt.Sprintf("DBT_KBC_%s_USER", targetUpper), o.Workspace.User)
	if len(o.PrivateKey) > 0 {
		// Write the private key to a separate file instead of inlining it in .env.local.
		// Inline PEM keys contain real newlines which cannot be represented portably in
		// dotenv format across bash, PowerShell, direnv, and dotenv libraries.
		keyFileName := fmt.Sprintf(".dbt_private_key_%s.p8", strings.ToLower(o.TargetName))
		keyFilePath := filesystem.Join(dbtProject.Fs().WorkingDir(), keyFileName)
		keyFile := filesystem.NewRawFile(keyFilePath, o.PrivateKey).SetDescription("dbt private key")
		if err := d.Fs().WriteFile(ctx, keyFile); err != nil {
			return errors.Errorf("cannot write file \"%s\": %w", keyFilePath, err)
		}
		envVars[fmt.Sprintf("DBT_KBC_%s_PRIVATE_KEY_PATH", targetUpper)] = keyFileName
		if err := addToGitignore(ctx, dbtProject.Fs(), keyFileName); err != nil {
			return err
		}
	}

	// Keboola adapter vars — written when the workspace was created via an editor session.
	if len(o.Workspace.BaseURL) > 0 && len(o.Workspace.WorkspaceID) > 0 {
		envVars[fmt.Sprintf("DBT_KBC_%s_BASE_URL", targetUpper)] = o.Workspace.BaseURL
		envVars[fmt.Sprintf("DBT_KBC_%s_BRANCH_ID", targetUpper)] = o.Workspace.BranchID.String()
		envVars[fmt.Sprintf("DBT_KBC_%s_WORKSPACE_ID", targetUpper)] = o.Workspace.WorkspaceID
		if token := d.StorageAPIToken().Token; token != "" {
			envVars["KEBOOLA_TOKEN"] = token
		}
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
		if strings.ContainsAny(v, " #\"'\\\t") {
			v = "\"" + strings.ReplaceAll(v, "\"", `\"`) + "\""
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

// addToGitignore appends entry to the project .gitignore if it is not already present.
func addToGitignore(ctx context.Context, fs filesystem.Fs, entry string) error {
	const gitignorePath = ".gitignore"
	existing := ""
	if fs.Exists(ctx, gitignorePath) {
		f, err := fs.FileLoader().ReadRawFile(ctx, filesystem.NewFileDef(gitignorePath))
		if err != nil {
			return errors.Errorf("cannot read %s: %w", gitignorePath, err)
		}
		existing = f.Content
	}
	for _, line := range strings.Split(existing, "\n") {
		if strings.TrimSpace(line) == entry {
			return nil
		}
	}
	updated := strings.TrimRight(existing, "\n")
	if updated != "" {
		updated += "\n"
	}
	updated += entry + "\n"
	if err := fs.WriteFile(ctx, filesystem.NewRawFile(gitignorePath, updated).SetDescription(".gitignore")); err != nil {
		return errors.Errorf("cannot write %s: %w", gitignorePath, err)
	}
	return nil
}
