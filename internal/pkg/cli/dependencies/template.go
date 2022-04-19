package dependencies

import (
	"fmt"
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
)

func (v *container) LocalTemplateExists() bool {
	// Get repository dir
	fs, err := v.localTemplateRepositoryDir()
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

func (v *container) LocalTemplate() (*template.Template, error) {
	// Get repository dir
	repository, err := v.LocalTemplateRepository()
	if err != nil {
		return nil, err
	}

	// Get working dir, inside repository dir
	workingDir := repository.Fs().WorkingDir()
	if len(workingDir) == 0 {
		return nil, ErrTemplateDirFound
	}

	// Template dir is [template]/[version], for example "my-template/v1".
	// Working dir must be the template dir or a subdir.
	parts := strings.SplitN(workingDir, string(filesystem.PathSeparator), 3)
	if len(parts) < 2 {
		return nil, fmt.Errorf(`directory "%s" is not a template directory`, parts[0:2])
	}
	templatePath := parts[0]

	// Get template
	templateRecord, found := repository.GetTemplateByPath(templatePath)
	if !found {
		return nil, fmt.Errorf(`template "%s" not found`, templatePath)
	}

	// Parse version
	version, err := model.NewSemVersion(parts[1])
	if err != nil {
		return nil, fmt.Errorf(`template version dir is invalid: %w`, err)
	}

	// Get version
	versionRecord, found := templateRecord.GetVersion(version)
	if !found {
		return nil, fmt.Errorf(`template "%s" found, but version "%s" is missing`, templatePath, version.Original())
	}

	// Check if template manifest exists
	manifestPath := filesystem.Join(versionRecord.Path(), template.ManifestPath())
	if !repository.Fs().IsFile(manifestPath) {
		return nil, ErrTemplateManifestNotFound
	}

	return v.Template(model.NewTemplateRef(localTemplateRepository(), templateRecord.Id, versionRecord.Version))
}
