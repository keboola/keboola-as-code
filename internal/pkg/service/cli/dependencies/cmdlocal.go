package dependencies

import (
	"context"
	"os"
	"path/filepath"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectPkg "github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/project/cachefile"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/version"
	loadTemplateOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/load"
	loadRepositoryOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/repository/load"
)

// localCommandScope implements LocalCommandScope interface.
type localCommandScope struct {
	dependencies.PublicScope
	BaseScope

	projectBackends []string
	projectFeatures keboola.FeaturesMap

	components              dependencies.Lazy[*model.ComponentsMap]
	localProject            dependencies.Lazy[localProjectValue]
	localTemplate           dependencies.Lazy[localTemplateValue]
	localTemplateRepository dependencies.Lazy[localRepositoryValue]
}

func (v *localCommandScope) ProjectBackends() []string {
	return v.projectBackends
}

func (v *localCommandScope) ProjectFeatures() keboola.FeaturesMap {
	return v.projectFeatures
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

func newLocalCommandScope(ctx context.Context, baseScp BaseScope, hostByFlag configmap.Value[string], opts ...Option) (*localCommandScope, error) {
	cfg := newConfig(opts)

	// Get Storage API host
	host, err := storageAPIHost(ctx, baseScp, cfg.defaultStorageAPIHost, hostByFlag)
	if err != nil {
		return nil, err
	}

	fileContent, err := cachefile.Load(ctx, baseScp.Fs())
	if err != nil {
		return nil, err
	}

	// Create common local dependencies
	pubScp, err := dependencies.NewPublicScope(
		ctx, baseScp, host,
		dependencies.WithPreloadComponents(true),
	)
	if err != nil {
		return nil, err
	}

	return &localCommandScope{
		PublicScope:     pubScp,
		BaseScope:       baseScp,
		projectFeatures: fileContent.Features.ToMap(),
		projectBackends: fileContent.Backends,
	}, nil
}

func (v *localCommandScope) Components() *model.ComponentsMap {
	// Use the same version of the components during the entire CLI command
	return v.components.MustInitAndGet(func() *model.ComponentsMap {
		return v.PublicScope.Components()
	})
}

func (v *localCommandScope) Template(ctx context.Context, reference model.TemplateRef) (*template.Template, error) {
	return v.TemplateForTests(ctx, reference, "")
}

func (v *localCommandScope) TemplateForTests(ctx context.Context, reference model.TemplateRef, projectFilesPath string) (*template.Template, error) {
	// Load repository
	repo, err := v.templateRepository(ctx, reference.Repository(), loadRepositoryOp.OnlyForTemplate(reference))
	if err != nil {
		return nil, err
	}

	// Update repository reference
	reference.WithRepository(repo.Definition())

	// Set working directory
	workDir := v.GlobalFlags().WorkingDir.Value
	if workDir == "" {
		workDir, err = os.Getwd() // nolint:forbidigo
		if err != nil {
			return nil, err
		}
	}
	// Set TestProjectFile
	var path string
	if projectFilesPath != "" {
		path = filepath.Join(workDir, projectFilesPath) // nolint:forbidigo
		if !filepath.IsAbs(path) {                      // nolint:forbidigo
			return nil, errors.Errorf("invalid path to projects file: %q", path)
		}
	}

	// Load template
	return loadTemplateOp.Run(ctx, v, repo, reference, path)
}

func (v *localCommandScope) LocalProject(ctx context.Context, ignoreErrors bool) (*projectPkg.Project, bool, error) {
	// Check version field
	value, err := v.localProject.InitAndGet(func() (localProjectValue, error) {
		fs, found, err := v.FsInfo().ProjectDir(ctx)
		if err != nil {
			return localProjectValue{found: found}, err
		}

		// Check manifest compatibility
		if err := version.CheckManifestVersion(ctx, v.Logger(), fs, projectManifest.Path()); err != nil {
			return localProjectValue{found: true}, err
		}

		// Create local instance
		p, err := projectPkg.New(ctx, v.Logger(), fs, v.Environment(), ignoreErrors)
		return localProjectValue{found: found, value: p}, err
	})
	return value.value, value.found, err
}

func (v *localCommandScope) LocalTemplateRepository(ctx context.Context) (*repository.Repository, bool, error) {
	value, err := v.localTemplateRepository.InitAndGet(func() (localRepositoryValue, error) {
		reference, found, err := v.localTemplateRepositoryRef(ctx)
		if err != nil {
			return localRepositoryValue{found: found}, err
		}
		repo, err := v.templateRepository(ctx, reference)
		return localRepositoryValue{found: true, value: repo}, err
	})
	return value.value, value.found, err
}

func (v *localCommandScope) LocalTemplate(ctx context.Context) (*template.Template, bool, error) {
	value, err := v.localTemplate.InitAndGet(func() (localTemplateValue, error) {
		// Get template path from current working dir
		paths, err := v.FsInfo().TemplatePath(ctx)
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
			return localTemplateValue{found: false}, errors.Errorf(`template with path "%s" not found in "%s"`, paths.TemplateDirName, repo.Manifest().Path())
		}

		// Get version
		versionRecord, found := templateRecord.GetByPath(paths.VersionDirName)
		if !found {
			return localTemplateValue{found: false}, errors.Errorf(`template "%s" found, but version directory "%s" is missing`, templateRecord.Name, paths.VersionDirName)
		}

		// Load template
		tmpl, err := v.Template(ctx, model.NewTemplateRef(repo.Definition(), templateRecord.ID, versionRecord.Version.String()))
		if err != nil {
			return localTemplateValue{found: true, value: tmpl}, err
		}

		// Set working directory
		if workingDir, err := filepath.Rel(tmpl.Fs().BasePath(), filepath.Join(v.Fs().BasePath(), filesystem.FromSlash(v.Fs().WorkingDir()))); err == nil { // nolint: forbidigo
			tmpl.Fs().SetWorkingDir(ctx, workingDir)
		}

		return localTemplateValue{found: true, value: tmpl}, nil
	})

	return value.value, value.found, err
}

func (v *localCommandScope) templateRepository(ctx context.Context, reference model.TemplateRepository, _ ...loadRepositoryOp.Option) (*repository.Repository, error) {
	// Handle CLI only features
	reference = v.mapRepositoryRelPath(ctx, reference)

	// Load repository
	repo, err := loadRepositoryOp.Run(ctx, v, reference)
	if err != nil {
		return nil, err
	}

	// Set working directory to repo FS
	if repo.Fs().BasePath() == v.Fs().BasePath() {
		repo.Fs().SetWorkingDir(ctx, v.Fs().WorkingDir())
	}

	return repo, nil
}

// mapRepositoryRelPath adds support for relative repository path.
// This feature is only available in the CLI.
func (v *localCommandScope) mapRepositoryRelPath(ctx context.Context, reference model.TemplateRepository) model.TemplateRepository {
	if reference.Type == model.RepositoryTypeDir {
		// Convert relative path to absolute
		if !filepath.IsAbs(reference.URL) && v.FsInfo().ProjectExists(ctx) { // nolint: forbidigo
			// Relative to the remote directory
			reference.URL = filepath.Join(v.Fs().BasePath(), reference.URL) // nolint: forbidigo
		}
	}
	return reference
}

func (v *localCommandScope) localTemplateRepositoryRef(ctx context.Context) (model.TemplateRepository, bool, error) {
	// Get repository dir
	fs, exists, err := v.FsInfo().TemplateRepositoryDir(ctx)
	if err != nil {
		return model.TemplateRepository{}, exists, err
	}
	// Create repository reference
	return model.TemplateRepository{Name: "keboola", Type: model.RepositoryTypeDir, URL: fs.BasePath()}, true, nil
}
