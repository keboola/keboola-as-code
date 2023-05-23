package load

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository/fs"
	loadRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/load"
)

type dependencies interface {
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

type config struct {
	fs              filesystem.Fs
	onlyForTemplate model.TemplateRef
}

type Option func(config *config)

// WithFs option set the repository FS instead of automatic loading.
// It is used by manager.Manager, which manages manager.CachedRepository in temporary directory.
func WithFs(fs filesystem.Fs) Option {
	return func(config *config) {
		config.fs = fs
	}
}

// OnlyForTemplate option modifies loading so that only the one template directory (+ common dir) is loaded, not all templates.
func OnlyForTemplate(ref model.TemplateRef) Option {
	return func(config *config) {
		config.onlyForTemplate = ref
	}
}

func Run(ctx context.Context, d dependencies, ref model.TemplateRepository, opts ...Option) (repo *repository.Repository, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.repository.load")
	defer span.End(&err)

	// Create config and apply options
	cnf := config{}
	for _, o := range opts {
		o(&cnf)
	}

	// Get FS
	root := cnf.fs
	if root == nil {
		root, err = fs.For(ctx, d, ref, fs.OnlyForTemplate(cnf.onlyForTemplate))
		if err != nil {
			return nil, err
		}
	}

	// Load manifest
	manifest, err := loadRepositoryManifest.Run(ctx, root, d)
	if err != nil {
		return nil, err
	}

	// FS for the optional common dir.
	// It contains common files that can be imported into all templates.
	var commonDir filesystem.Fs
	if root.IsDir(repository.CommonDirectory) {
		if v, err := root.SubDirFs(repository.CommonDirectory); err == nil {
			commonDir = v
		} else {
			return nil, err
		}
	} else {
		commonDir = aferofs.NewMemoryFs()
	}

	return repository.New(ref, root, commonDir, manifest), nil
}
