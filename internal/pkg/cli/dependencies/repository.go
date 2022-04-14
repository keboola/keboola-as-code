package dependencies

import (
	"fmt"
	"path/filepath"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/git"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	repositoryManifest "github.com/keboola/keboola-as-code/internal/pkg/template/repository/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	loadRepositoryManifest "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/repository/manifest/load"
)

func localTemplateRepository() model.TemplateRepository {
	return model.TemplateRepository{Type: model.RepositoryTypeWorkingDir}
}

func (v *container) LocalTemplateRepositoryExists() bool {
	return v.fs.IsFile(repositoryManifest.Path())
}

func (v *container) LocalTemplateRepository() (*repository.Repository, error) {
	return v.TemplateRepository(localTemplateRepository(), nil)
}

func (v *container) Template(reference model.TemplateRef) (*template.Template, error) {
	return v.commonDeps.Template(reference)
}

func (v *container) TemplateRepository(definition model.TemplateRepository, forTemplate model.TemplateRef) (*repository.Repository, error) {
	fs, err := v.repositoryFs(definition, forTemplate)
	if err != nil {
		return nil, err
	}
	manifest, err := loadRepositoryManifest.Run(fs, v)
	if err != nil {
		return nil, err
	}
	return repository.New(definition, fs, manifest), nil
}

func (v *container) repositoryFs(definition model.TemplateRepository, template model.TemplateRef) (filesystem.Fs, error) {
	switch definition.Type {
	case model.RepositoryTypeWorkingDir:
		fs, err := v.localTemplateRepositoryDir()
		if err != nil {
			return nil, err
		}

		// Convert RepositoryTypeWorkingDir -> RepositoryTypeDir.
		// So it can be loaded in a common way.
		definition = model.TemplateRepository{
			Type:       model.RepositoryTypeDir,
			Name:       definition.Name,
			Path:       fs.BasePath(),
			WorkingDir: fs.WorkingDir(),
		}
		fallthrough // continue with RepositoryTypeDir
	case model.RepositoryTypeDir:
		path := definition.Path
		// Convert relative path to absolute
		if !filepath.IsAbs(path) && v.LocalProjectExists() { // nolint: forbidigo
			// Relative to the project directory
			path = filepath.Join(v.fs.BasePath(), path) // nolint: forbidigo
		}
		return aferofs.NewLocalFs(v.Logger(), path, definition.WorkingDir)
	case model.RepositoryTypeGit:
		return gitRepositoryFs(definition, template, v.Logger())
	default:
		panic(fmt.Errorf(`unexpected repository type "%s"`, definition.Type))
	}
}

// gitRepositoryFs returns template repository FS, which has been loaded from a git repository.
// Sparse checkout is used to load only the needed files.
func gitRepositoryFs(definition model.TemplateRepository, tmplRef model.TemplateRef, logger log.Logger) (filesystem.Fs, error) {
	// Checkout Git repository in sparse mode
	gitRepository, err := git.Checkout(definition.Url, definition.Ref, true, logger)
	if err != nil {
		return nil, err
	}

	// Clear temp directory at the end.
	// Files will be copied to memory.
	defer gitRepository.Clear()

	// Load repository manifest
	if err := gitRepository.Load(".keboola/repository.json"); err != nil {
		return nil, err
	}
	repoManifest, err := repository.LoadManifest(gitRepository.Fs())
	if err != nil {
		return nil, err
	}

	// Get version record
	_, version, err := repoManifest.GetVersion(tmplRef.TemplateId(), tmplRef.Version())
	if err != nil {
		// version or template not found
		e := utils.NewMultiError()
		e.Append(fmt.Errorf(`searched in git repository "%s"`, gitRepository.Url()))
		e.Append(fmt.Errorf(`reference "%s"`, gitRepository.Ref()))
		return nil, utils.PrefixError(err.Error(), e)
	}

	// Load template src directory
	srcDir := filesystem.Join(version.Path(), template.SrcDirectory)
	if err := gitRepository.Load(srcDir); err != nil {
		return nil, err
	}
	if !gitRepository.Fs().Exists(srcDir) {
		e := utils.NewMultiError()
		e.Append(fmt.Errorf(`searched in git repository "%s"`, gitRepository.Url()))
		e.Append(fmt.Errorf(`reference "%s"`, gitRepository.Ref()))
		return nil, utils.PrefixError(fmt.Sprintf(`folder "%s" not found`, srcDir), e)
	}

	// Load common directory, shared between all templates in repository, if it exists
	if err := gitRepository.Load(template.CommonDirectory); err != nil {
		return nil, err
	}

	// Copy to memory FS, temp dir will be cleared
	memoryFs, err := aferofs.NewMemoryFs(logger, "")
	if err != nil {
		return nil, err
	}
	if err := aferofs.CopyFs2Fs(gitRepository.Fs(), "", memoryFs, ""); err != nil {
		return nil, err
	}
	return memoryFs, nil
}

func (v *container) localTemplateRepositoryDir() (filesystem.Fs, error) {
	if !v.LocalTemplateRepositoryExists() {
		if v.LocalProjectExists() {
			return nil, ErrExpectedRepositoryFoundProject
		}
		return nil, ErrRepositoryManifestNotFound
	}
	return v.fs, nil
}
