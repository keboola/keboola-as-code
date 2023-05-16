package fs

import (
	"context"
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// gitFsFor returns template FS loaded from a git repository.
// Sparse checkout is used to load only the needed files.
func gitFsFor(ctx context.Context, d dependencies, definition model.TemplateRepository, opts ...Option) (memoryFs filesystem.Fs, err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.declarative.templates.repository.fs.gitFsFor")
	defer telemetry.EndSpan(span, &err)

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Apply options
	config := config{}
	for _, o := range opts {
		o(&config)
	}

	// If we load the repository only for one template, we don't need all the files
	sparse := config.onlyForTemplate != nil

	// Checkout Git repository in sparse mode
	gitRepository, err := git.Checkout(ctx, definition, sparse, d.Logger())
	if err != nil {
		return nil, err
	}

	// Clear directory at the end. Files will be copied to memory.
	defer func() {
		<-gitRepository.Free()
	}()

	// Get repository FS
	// WorkingFs() is used, because we are going to add more dirs, if sparse = true.
	// It would be pointless to call Fs() after every change to get the actual version of the repository.
	fs := gitRepository.WorkingFs()

	if sparse {
		// Add repository manifest to sparse git repository
		if err := gitRepository.Load(ctx, ".keboola/repository.json"); err != nil {
			return nil, err
		}

		// Load repository manifest
		repoManifest, err := repository.LoadManifest(fs)
		if err != nil {
			return nil, err
		}

		// Get version record
		tmpl := config.onlyForTemplate
		_, version, err := repoManifest.GetVersion(tmpl.TemplateID(), tmpl.Version())
		if err != nil {
			// version or template not found
			return nil, errors.NewNestedError(
				err,
				errors.Errorf(`searched in git repository "%s"`, gitRepository.URL()),
				errors.Errorf(`reference "%s"`, gitRepository.Ref()),
			)
		}

		// Load template src directory
		srcDir := filesystem.Join(version.Path(), template.SrcDirectory)
		if err := gitRepository.Load(ctx, srcDir); err != nil {
			return nil, err
		}
		if !fs.Exists(srcDir) {
			return nil, errors.NewNestedError(
				errors.Errorf(`folder "%s" not found`, srcDir),
				errors.Errorf(`searched in git repository "%s"`, gitRepository.URL()),
				errors.Errorf(`reference "%s"`, gitRepository.Ref()),
			)
		}

		// Load common directory, shared between all templates in repository, if it exists
		if err := gitRepository.Load(ctx, repository.CommonDirectory); err != nil {
			return nil, err
		}
	}

	// Copy to memory FS, temp dir will be cleared
	memoryFs = aferofs.NewMemoryFs(filesystem.WithLogger(d.Logger()))
	if err := aferofs.CopyFs2Fs(fs, "", memoryFs, ""); err != nil {
		return nil, err
	}
	return memoryFs, nil
}
