package init

import (
	"context"
	"time"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/dbt"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/crypto"
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

	// Optionally generate Snowflake key-pair
	var privateKeyPEM string
	var publicKeyPEM string
	if o.UseKeyPair {
		if privateKeyPEM, publicKeyPEM, err = crypto.GenerateRSAKeyPairPEM(); err != nil {
			return errors.Errorf("cannot generate key-pair: %w", err)
		}
	}

	// Create workspace
	d.Logger().Info(ctx, `Creating a new workspace, please wait.`)
	createOpts := make([]keboola.CreateSandboxWorkspaceOption, 0)
	if o.UseKeyPair {
		// Pass public key to enable key-pair authentication in the workspace
		createOpts = append(createOpts, keboola.WithPublicKey(publicKeyPEM))
	}
	w, err := d.KeboolaProjectAPI().CreateSandboxWorkspace(
		ctx,
		branch.ID,
		o.WorkspaceName,
		keboola.SandboxWorkspaceTypeSnowflake,
		createOpts...,
	)
	if err != nil {
		return errors.Errorf("cannot create workspace: %w", err)
	}
	d.Logger().Infof(ctx, `Created the new workspace "%s".`, o.WorkspaceName)
	workspace := w.SandboxWorkspace

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
		PrivateKey: privateKeyPEM,
		UseKeyPair: o.UseKeyPair,
		Buckets:    buckets,
	}, d)
	if err != nil {
		return errors.Errorf("could not generate env: %w", err)
	}

	return nil
}
