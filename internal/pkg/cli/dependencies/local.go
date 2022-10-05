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
	loadTemplateOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/load"
	loadRepositoryOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/repository/load"
)

// local dependencies container implements ForLocalCommand interface.
type local struct {
	dependencies.Public
	Base
	components              dependencies.Lazy[*model.ComponentsMap]
	localProject            dependencies.Lazy[localProjectValue]
	localTemplate           dependencies.Lazy[localTemplateValue]
	localTemplateRepository dependencies.Lazy[localRepositoryValue]
}

type localProjectValue struct {
	found bool
	value *projectPkg.Project
}

type localRepositoryValue struct {
	found bool
	value *repository.Repository
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

func (v *local) Template(ctx context.Context, reference model.TemplateRef) (*template.Template, error) {
	// Load repository
	repo, err := v.templateRepository(ctx, reference.Repository(), loadRepositoryOp.OnlyForTemplate(reference))
	if err != nil {
		return nil, err
	}

	// Update repository reference
	reference.WithRepository(repo.Definition())

	// Load template
	return loadTemplateOp.Run(ctx, v, repo, reference)
}

func (v *local) LocalProject(ignoreErrors bool) (*projectPkg.Project, bool, error) {
	// Check version field
	value, err := v.localProject.InitAndGet(func() (localProjectValue, error) {
		fs, found, err := v.FsInfo().ProjectDir()
		if err != nil {
			return localProjectValue{found: found}, err
		}

		// Check manifest compatibility
		if err := version.CheckManifestVersion(v.Logger(), fs, projectManifest.Path()); err != nil {
			return localProjectValue{found: true}, err
		}

		// Create remote instance
		p, err := projectPkg.New(v.CommandCtx(), fs, ignoreErrors)
		return localProjectValue{found: found, value: p}, err
	})
	return value.value, value.found, err
}

func (v *local) LocalTemplateRepository(ctx context.Context) (*repository.Repository, bool, error) {
	value, err := v.localTemplateRepository.InitAndGet(func() (localRepositoryValue, error) {
		reference, found, err := v.localTemplateRepositoryRef()
		if err != nil {
			return localRepositoryValue{found: found}, err
		}
		repo, err := v.templateRepository(ctx, reference)
		return localRepositoryValue{found: true, value: repo}, err
	})
	return value.value, value.found, err
}

func (v *local) LocalTemplate(ctx context.Context) (*template.Template, bool, error) {
	value, err := v.localTemplate.InitAndGet(func() (localTemplateValue, error) {
		// Get template path from current working dir
		paths, err := v.FsInfo().TemplatePath()
		if err != nil {
			return localTemplateValue{found: false}, err
		}

		// Get repository dir
		repo, _, err := v.LocalTemplateRepository(ctx)
		if err != nil {
			return localTemplateValue{found: false}, err
		}

		// Get template
		templateRecord, found := repo.RecordByPath(paths.TemplateDirName)
		if !found {
			return localTemplateValue{found: false}, fmt.Errorf(`template with path "%s" not found in "%s"`, paths.TemplateDirName, repo.Manifest().Path())
		}

		// Get version
		versionRecord, found := templateRecord.GetByPath(paths.VersionDirName)
		if !found {
			return localTemplateValue{found: false}, fmt.Errorf(`template "%s" found, but version directory "%s" is missing`, templateRecord.Name, paths.VersionDirName)
		}

		// Load template
		tmpl, err := v.Template(ctx, model.NewTemplateRef(repo.Definition(), templateRecord.Id, versionRecord.Version.String()))
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

func (v *local) templateRepository(ctx context.Context, reference model.TemplateRepository, _ ...loadRepositoryOp.Option) (*repository.Repository, error) {
	// Handle CLI only features
	reference = v.mapRepositoryRelPath(reference)

	// Load repository
	repo, err := loadRepositoryOp.Run(ctx, v, reference)
	if err != nil {
		return nil, err
	}

	// Set working directory to repo FS
	if repo.Fs().BasePath() == v.Fs().BasePath() {
		repo.Fs().SetWorkingDir(v.Fs().WorkingDir())
	}

	return repo, nil
}

// mapRepositoryRelPath adds support for relative repository path.
// This feature is only available in the CLI.
func (v *local) mapRepositoryRelPath(reference model.TemplateRepository) model.TemplateRepository {
	if reference.Type == model.RepositoryTypeDir {
		// Convert relative path to absolute
		if !filepath.IsAbs(reference.Url) && v.FsInfo().ProjectExists() { // nolint: forbidigo
			// Relative to the remote directory
			reference.Url = filepath.Join(v.Fs().BasePath(), reference.Url) // nolint: forbidigo
		}
	}
	return reference
}

func (v *local) localTemplateRepositoryRef() (model.TemplateRepository, bool, error) {
	// Get repository dir
	fs, exists, err := v.FsInfo().TemplateRepositoryDir()
	if err != nil {
		return model.TemplateRepository{}, exists, err
	}
	// Create repository reference
	return model.TemplateRepository{Name: "keboola", Type: model.RepositoryTypeDir, Url: fs.BasePath()}, true, nil
}
