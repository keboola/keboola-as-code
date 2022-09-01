package dependencies

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectPkg "github.com/keboola/keboola-as-code/internal/pkg/project"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
)

// local dependencies container implements ForLocalCommand interface.
type local struct {
	dependencies.Public
	Base
	components              dependencies.Lazy[*model.ComponentsMap]
	localProject            dependencies.Lazy[*projectPkg.Project]
	localTemplate           dependencies.Lazy[localTemplateValue]
	localTemplateRepository dependencies.Lazy[localRepositoryValue]
}

type localTemplateValue struct {
	found bool
	value *template.Template
}

func newPublicDeps(baseDeps Base) (*local, error) {
	// Get Storage API host
	host, err := storageApiHost(baseDeps.Fs(), baseDeps.Options())
	if err != nil {
		return nil, err
	}

	// Create common local dependencies
	publicDep, err := dependencies.NewPublicDeps(baseDeps.CommandCtx(), baseDeps, host)
	if err != nil {
		return nil, err
	}

	return &local{
		Public: publicDep,
		Base:   baseDeps,
	}, nil
}

func (v *local) Components() *model.ComponentsMap {
	// Use the same version of the components during the entire CLI command
	return v.components.MustInitAndGet(func() *model.ComponentsMap {
		return v.Public.Components()
	})
}

func (v *local) LocalProject(ignoreErrors bool) (*projectPkg.Project, bool, error) {
	// Check if the working directory is a project directory
	if !v.localProject.IsSet() && !v.Fs().IsFile(projectManifest.Path()) {
		if v.FsInfo().LocalTemplateExists() {
			return nil, false, ErrExpectedProjectFoundTemplate
		}
		if v.FsInfo().LocalTemplateRepositoryExists() {
			return nil, false, ErrExpectedProjectFoundRepository
		}
		return nil, false, ErrProjectManifestNotFound
	}

	// Check version field
	p, err := v.localProject.InitAndGet(func() (*projectPkg.Project, error) {
		// Check manifest compatibility
		if err := version.CheckManifestVersion(v.Logger(), v.Fs(), projectManifest.Path()); err != nil {
			return nil, err
		}

		// Create remote instance
		return projectPkg.New(v.CommandCtx(), v.Fs(), ignoreErrors)
	})
	return p, true, err
}

type localRepositoryValue struct {
	found bool
	value *repository.Repository
}

func (v *local) LocalTemplateRepository(ctx context.Context) (*repository.Repository, bool, error) {
	value, err := v.localTemplateRepository.InitAndGet(func() (localRepositoryValue, error) {
		reference, found, err := v.localTemplateRepositoryRef()
		if err != nil {
			return localRepositoryValue{found: found}, err
		}
		repo, err := v.TemplateRepository(ctx, reference, nil)
		return localRepositoryValue{found: true, value: repo}, err
	})
	return value.value, value.found, err
}

func (v *local) Template(ctx context.Context, reference model.TemplateRef) (*template.Template, error) {
	if repo, err := v.mapRepositoryRelPath(reference.Repository()); err != nil {
		return nil, err
	} else {
		return v.Public.Template(ctx, reference.WithRepository(repo))
	}
}

func (v *local) TemplateRepository(ctx context.Context, reference model.TemplateRepository, forTemplate model.TemplateRef) (*repository.Repository, error) {
	// Handle CLI only features
	modifiedReference, err := v.mapRepositoryRelPath(reference)
	if err != nil {
		return nil, err
	}

	// Update template reference
	if forTemplate != nil {
		forTemplate = forTemplate.WithRepository(modifiedReference)
	}

	// Get repository
	repo, err := v.Public.TemplateRepository(ctx, modifiedReference, forTemplate)
	if err != nil {
		return nil, err
	}

	// Set working directory to repo FS
	if repo.Fs().BasePath() == v.Fs().BasePath() {
		repo.Fs().SetWorkingDir(v.Fs().WorkingDir())
	}

	return repo, nil
}

func (v *local) LocalTemplate(ctx context.Context) (*template.Template, bool, error) {
	value, err := v.localTemplate.InitAndGet(func() (localTemplateValue, error) {
		// Get template path from current working dir
		paths, err := v.FsInfo().LocalTemplatePath()
		if err != nil {
			return localTemplateValue{found: false}, err
		}

		// Get repository dir
		repo, _, err := v.LocalTemplateRepository(ctx)
		if err != nil {
			return localTemplateValue{found: false}, err
		}

		// Get template
		templateRecord, found := repo.GetTemplateByPath(paths.TemplateDirName)
		if !found {
			return localTemplateValue{found: false}, fmt.Errorf(`template with path "%s" not found in "%s"`, paths.TemplateDirName, repo.Manifest().Path())
		}

		// Get version
		versionRecord, found := templateRecord.GetByPath(paths.VersionDirName)
		if !found {
			return localTemplateValue{found: false}, fmt.Errorf(`template "%s" found, but version directory "%s" is missing`, templateRecord.Name, paths.VersionDirName)
		}

		// Load template
		tmpl, err := v.Template(ctx, model.NewTemplateRef(repo.Ref(), templateRecord.Id, versionRecord.Version.String()))
		if err != nil {
			return localTemplateValue{found: true, value: tmpl}, err
		}

		// Set working directory
		if workingDir, err := filepath.Rel(tmpl.Fs().BasePath(), filepath.Join(v.Fs().BasePath(), filesystem.FromSlash(v.Fs().WorkingDir()))); err == nil { // nolint: forbidigo
			tmpl.Fs().SetWorkingDir(workingDir)
		}

		return localTemplateValue{found: true, value: tmpl}, nil
	})

	return value.value, value.found, err
}

// mapRepositoryRelPath adds support for relative repository path.
// This feature is only available in the CLI.
func (v *local) mapRepositoryRelPath(reference model.TemplateRepository) (model.TemplateRepository, error) {
	if reference.Type == model.RepositoryTypeDir {
		// Convert relative path to absolute
		if !filepath.IsAbs(reference.Url) && v.FsInfo().LocalProjectExists() { // nolint: forbidigo
			// Relative to the remote directory
			reference.Url = filepath.Join(v.Fs().BasePath(), reference.Url) // nolint: forbidigo
		}
	}
	return reference, nil
}

func (v *local) localTemplateRepositoryRef() (model.TemplateRepository, bool, error) {
	// Get repository dir
	fs, exists, err := v.FsInfo().LocalTemplateRepositoryDir()
	if err != nil {
		return model.TemplateRepository{}, exists, err
	}
	// Create repository reference
	return model.TemplateRepository{Name: "keboola", Type: model.RepositoryTypeDir, Url: fs.BasePath()}, true, nil
}
