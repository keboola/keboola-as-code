package dependencies

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	createTemplateDir "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/dir/create"
	loadInputsOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/local/inputs/load"
	loadStateOp "github.com/keboola/keboola-as-code/pkg/lib/operation/template/state/load"
)

var ErrTemplateManifestNotFound = fmt.Errorf("template manifest not found")

func (c *common) LocalTemplateExists() bool {
	// Get repository dir
	fs, err := c.LocalTemplateRepositoryDir()
	if err != nil {
		return false
	}

	// Template dir is [template]/[version], for example "my-template/v1".
	// Working dir must be the template dir or a subdir.
	parts := strings.SplitN(fs.WorkingDir(), string(filesystem.PathSeparator), 3)
	if len(parts) < 2 {
		return false
	}

	templateDir := strings.Join(parts[0:2], string(filesystem.PathSeparator))
	manifestPath := filesystem.Join(templateDir, templateManifest.Path())
	return fs.IsFile(manifestPath)
}

func (c *common) LocalTemplate() (*template.Template, error) {
	// Get repository dir
	repository, err := c.LocalTemplateRepository()
	if err != nil {
		return nil, err
	}

	// Template dir is [template]/[version], for example "my-template/v1".
	// Working dir must be the template dir or a subdir.
	parts := strings.SplitN(repository.Fs().WorkingDir(), string(filesystem.PathSeparator), 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf(`directory "%s" is not a template directory`, parts[0:2])
	}
	templatePath := parts[0]

	// Get template
	templateRecord, found := repository.GetByPath(templatePath)
	if !found {
		return nil, fmt.Errorf(`template "%s" not found`, templatePath)
	}

	// Parse version
	version, err := model.NewSemVersion(parts[1])
	if err != nil {
		return nil, fmt.Errorf(`template version dir is invalid: %w`, err)
	}

	// Get version
	versionRecord, found := templateRecord.GetByVersion(version)
	if !found {
		return nil, fmt.Errorf(`template "%s" found, but version "%s" is missing`, templatePath, version.Original())
	}

	return c.Template(model.TemplateRef{
		Id:         templateRecord.Id,
		Version:    versionRecord.Version.String(),
		Repository: localTemplateRepository(),
	})
}

func (c *common) Template(reference model.TemplateRef) (*template.Template, error) {
	// Load repository
	repository, err := c.TemplateRepository(reference.Repository, reference)
	if err != nil {
		return nil, err
	}

	// Get template
	templateRecord, found := repository.GetById(reference.Id)
	if !found {
		return nil, fmt.Errorf(`template "%s" not found`, reference.Id)
	}

	// Parse version
	version, err := model.NewSemVersion(reference.Version)
	if err != nil {
		return nil, fmt.Errorf(`template version dir is invalid: %w`, err)
	}

	// Get version
	versionRecord, found := templateRecord.GetByVersion(version)
	if !found {
		return nil, fmt.Errorf(`template "%s" found, but version "%s" is missing`, reference.Id, version.Original())
	}

	// Check if template dir exists
	templatePath := versionRecord.Path()
	if !repository.Fs().IsDir(templatePath) {
		return nil, fmt.Errorf(`template dir "%s" not found`, templatePath)
	}

	// Template dir
	fs, err := repository.Fs().SubDirFs(templatePath)
	if err != nil {
		return nil, err
	}

	// Load inputs
	inputs, err := loadInputsOp.Run(fs)
	if err != nil {
		return nil, err
	}

	return template.New(reference, fs, inputs)
}

func (c *common) TemplateState(options loadStateOp.Options) (*template.State, error) {
	return loadStateOp.Run(options, c)
}

func (c *common) CreateTemplateDir(repositoryDir filesystem.Fs, path string) (filesystem.Fs, error) {
	return createTemplateDir.Run(createTemplateDir.Options{RepositoryDir: repositoryDir, Path: path}, c)
}
