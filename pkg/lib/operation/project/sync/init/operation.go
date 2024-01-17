package init

import (
	"context"
	"io"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	createEnvFiles "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/envfiles/create"
	createManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/manifest/create"
	createMetaDir "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/metadir/create"
	genWorkflows "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/workflows/generate"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/pull"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	Pull            bool // run pull after init?
	ManifestOptions createManifest.Options
	Workflows       genWorkflows.Options
}

type dependencies interface {
	Components() *model.ComponentsMap
	EmptyDir(ctx context.Context) (filesystem.Fs, error)
	KeboolaProjectAPI() *keboola.API
	Logger() log.Logger
	Options() *options.Options
	ProjectID() keboola.ProjectID
	StorageAPIHost() string
	StorageAPIToken() keboola.Token
	Telemetry() telemetry.Telemetry
	Stdout() io.Writer
}

func Run(ctx context.Context, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.project.sync.init")
	defer span.End(&err)

	logger := d.Logger()

	fs, err := d.EmptyDir(ctx)
	if err != nil {
		return err
	}

	// Create metadata dir
	if err := createMetaDir.Run(ctx, fs, d); err != nil {
		return err
	}

	// Create manifest
	manifest, err := createManifest.Run(ctx, fs, o.ManifestOptions, d)
	if err != nil {
		return errors.Errorf(`cannot create manifest: %w`, err)
	}

	// Create ENV files
	if err := createEnvFiles.Run(ctx, fs, d); err != nil {
		return err
	}

	// Related operations
	errs := errors.NewMultiError()

	// Generate CI workflows
	if err := genWorkflows.Run(ctx, fs, o.Workflows, d); err != nil {
		errs.AppendWithPrefix(err, "workflows generation failed")
	}

	logger.Info(ctx, "Init done.")

	// First pull
	if o.Pull {
		logger.Info(ctx, "")
		logger.Info(ctx, `Running pull.`)

		// Load project state
		prj := project.NewWithManifest(ctx, fs, manifest)
		projectState, err := prj.LoadState(loadState.InitOptions(o.Pull), d)
		if err != nil {
			return err
		}

		// Pull
		pullOptions := pull.Options{DryRun: false, LogUntrackedPaths: false}
		if err := pull.Run(ctx, projectState, pullOptions, d); err != nil {
			errs.AppendWithPrefix(err, "pull failed")
		}
	}

	return errs.ErrorOrNil()
}
