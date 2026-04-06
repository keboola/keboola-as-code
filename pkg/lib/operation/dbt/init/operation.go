package init

import (
	"context"
	"strconv"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/env"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/profile"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/generate/sources"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/dbt/listbuckets"
)

type DbtInitOptions struct {
	BranchKey     keboola.BranchKey
	TargetName    string
	WorkspaceName string
	BaseURL       string // Keboola Query Service base URL, e.g. "https://query.keboola.com"
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	StorageAPIToken() keboola.Token
	LocalDbtProject(ctx context.Context) (*dbt.Project, bool, error)
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
	Fs() filesystem.Fs
}

func Run(ctx context.Context, o DbtInitOptions, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.dbt.init")
	defer span.End(&err)

	// Check that we are in dbt directory
	if _, _, err := d.LocalDbtProject(ctx); err != nil {
		return err
	}

	branch, err := d.KeboolaProjectAPI().GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeoutCause(ctx, 10*time.Minute, errors.New("dbt init timeout"))
	defer cancel()

	// Phase 1: Create editor session — provides workspace coordinates for the keboola_snowflake
	// dbt profile (WorkspaceID, BranchID, BaseURL). No credential rotation at this step.
	d.Logger().Info(ctx, `Creating a new workspace, please wait.`)
	session, err := d.KeboolaProjectAPI().CreateEditorSession(ctx, branch.ID, o.WorkspaceName)
	if err != nil {
		return errors.Errorf("cannot create workspace: %w", err)
	}
	d.Logger().Infof(ctx, `Created the new workspace "%s".`, o.WorkspaceName)

	// Phase 2: Create storage workspace credentials — server generates a keypair, registers
	// the public key with the workspace, and returns the private key together with all
	// connection details (Host, User, Database, Schema, Warehouse). These fill the
	// direct-Snowflake dbt profile. Password auth is deprecated; keypair is used instead.
	workspaceIDUint, err := strconv.ParseUint(session.EditorSession.WorkspaceID, 10, 64)
	if err != nil {
		return errors.Errorf("cannot parse workspace ID %q: %w", session.EditorSession.WorkspaceID, err)
	}
	storageWS, err := d.KeboolaProjectAPI().StorageWorkspaceCreateCredentialsRequest(branch.ID, workspaceIDUint).Send(ctx)
	if err != nil {
		return errors.Errorf("cannot fetch workspace credentials: %w", err)
	}

	// Build WorkspaceDetails combining both phases:
	// Phase 1 fields (keboola_snowflake profile): BaseURL, BranchID, WorkspaceID
	// Phase 2 fields (direct-Snowflake profile):  Host, User, Database, Schema, Warehouse, PrivateKey
	deref := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	workspace := env.WorkspaceDetails{
		Type:        string(storageWS.StorageWorkspaceDetails.Backend),
		Host:        deref(storageWS.StorageWorkspaceDetails.Host),
		User:        deref(storageWS.StorageWorkspaceDetails.User),
		Database:    deref(storageWS.StorageWorkspaceDetails.Database),
		Schema:      deref(storageWS.StorageWorkspaceDetails.Schema),
		Warehouse:   deref(storageWS.StorageWorkspaceDetails.Warehouse),
		BaseURL:     o.BaseURL,
		BranchID:    branch.ID,
		WorkspaceID: session.EditorSession.WorkspaceID,
	}

	// Determine private key from the freshly created credentials.
	privateKey := ""
	if storageWS.StorageWorkspaceDetails.PrivateKey != nil {
		privateKey = *storageWS.StorageWorkspaceDetails.PrivateKey
	}

	// List buckets
	buckets, err := listbuckets.Run(ctx, listbuckets.Options{
		BranchKey:  o.BranchKey,
		TargetName: o.TargetName,
	}, d)
	if err != nil {
		return errors.Errorf("could not list buckets: %w", err)
	}

	// Generate profile — always include the keboola_snowflake target because dbt init
	// creates a Snowflake workspace with an editor session, making all keboola_ env vars available.
	err = profile.Run(ctx, profile.Options{
		TargetName:           o.TargetName,
		IncludeKeboolaTarget: true,
	}, d)
	if err != nil {
		return errors.Errorf("could not generate profile: %w", err)
	}

	// Generate sources
	err = sources.Run(ctx, sources.Options{
		BranchKey:  o.BranchKey,
		TargetName: o.TargetName,
		Buckets:    buckets,
	}, d)
	if err != nil {
		return errors.Errorf("could not generate sources: %w", err)
	}

	// Generate env
	err = env.Run(ctx, env.Options{
		BranchKey:  o.BranchKey,
		TargetName: o.TargetName,
		Workspace:  workspace,
		PrivateKey: privateKey,
		Buckets:    buckets,
	}, d)
	if err != nil {
		return errors.Errorf("could not generate env: %w", err)
	}

	return nil
}
