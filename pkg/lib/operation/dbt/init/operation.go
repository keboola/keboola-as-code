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
	UseKeyPair    bool
	BaseURL       string // Keboola Query Service base URL, e.g. "https://query.keboola.com"
}

type dependencies interface {
	KeboolaProjectAPI() *keboola.AuthorizedAPI
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

	// Create SQL workspace via editor session — backend is determined by the project config.
	d.Logger().Info(ctx, `Creating a new workspace, please wait.`)
	session, err := d.KeboolaProjectAPI().CreateEditorSession(ctx, branch.ID, o.WorkspaceName)
	if err != nil {
		return errors.Errorf("cannot create workspace: %w", err)
	}
	d.Logger().Infof(ctx, `Created the new workspace "%s".`, o.WorkspaceName)

	// Create fresh credentials for the storage workspace to get connection details + private key.
	workspaceIDUint, err := strconv.ParseUint(session.EditorSession.WorkspaceID, 10, 64)
	if err != nil {
		return errors.Errorf("cannot parse workspace ID %q: %w", session.EditorSession.WorkspaceID, err)
	}
	storageWS, err := d.KeboolaProjectAPI().StorageWorkspaceCreateCredentialsRequest(branch.ID, workspaceIDUint).Send(ctx)
	if err != nil {
		return errors.Errorf("cannot fetch workspace credentials: %w", err)
	}

	// Build WorkspaceDetails from StorageWorkspace credentials.
	deref := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	workspace := env.WorkspaceDetails{
		Type:        keboola.SandboxWorkspaceType(storageWS.StorageWorkspaceDetails.Backend),
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
	if o.UseKeyPair && storageWS.StorageWorkspaceDetails.PrivateKey != nil {
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

	// Generate profile
	err = profile.Run(ctx, profile.Options{
		TargetName: o.TargetName,
		UseKeyPair: o.UseKeyPair,
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
		UseKeyPair: o.UseKeyPair,
		Buckets:    buckets,
	}, d)
	if err != nil {
		return errors.Errorf("could not generate env: %w", err)
	}

	return nil
}
