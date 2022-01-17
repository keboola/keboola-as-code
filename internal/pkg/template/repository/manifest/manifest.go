package manifest

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const FileName = `repository.json`

type Manifest struct {
	fs        filesystem.Fs
	changed   bool
	templates []*TemplateRecord
}

type TemplateRecord struct {
	Name          string `json:"name" validate:"required"`
	Description   string `json:"description" validate:"required"`
	model.AbsPath `validate:"dive"`
	Versions      []*VersionRecord `json:"versions" validate:"required,dive"`
}

type VersionRecord struct {
	Version       string `json:"version" validate:"required,semver"`
	Description   string `json:"description" validate:"required"`
	Stable        bool   `json:"stable" validate:"required"`
	model.AbsPath `validate:"dive"`
}

// file is repository manifest JSON file.
type file struct {
	Version   int               `json:"version" validate:"required,min=1,max=2"`
	Templates []*TemplateRecord `json:"templates"`
}

func New(fs filesystem.Fs) *Manifest {
	return &Manifest{
		fs:        fs,
		templates: make([]*TemplateRecord, 0),
	}
}

func Path() string {
	return filesystem.Join(filesystem.MetadataDir, FileName)
}

func (m *Manifest) Path() string {
	return Path()
}

func Load(fs filesystem.Fs) (*Manifest, error) {
	// Read manifest file
	manifestContent, err := loadManifestFile(fs)
	if err != nil {
		return nil, err
	}

	// Create manifest struct
	m := New(fs)
	m.templates = manifestContent.Templates

	// Track if manifest was changed after load
	m.changed = false

	// Return
	return m, nil
}

func (m *Manifest) Save() error {
	content := newFile()
	content.Templates = m.templates

	// Save manifest file
	if err := saveManifestFile(m.fs, content); err != nil {
		return err
	}

	m.changed = false
	return nil
}

func newFile() *file {
	return &file{
		Version:   build.MajorVersion,
		Templates: make([]*TemplateRecord, 0),
	}
}

func loadManifestFile(fs filesystem.Fs) (*file, error) {
	// Check if file exists
	path := Path()
	if !fs.IsFile(path) {
		return nil, fmt.Errorf("manifest \"%s\" not found", path)
	}

	// Read JSON file
	manifestContent := newFile()
	if err := fs.ReadJsonFileTo(path, "manifest", manifestContent); err != nil {
		return nil, err
	}

	// Fill in parent paths
	for _, template := range manifestContent.Templates {
		template.AbsPath.SetParentPath(``)
		for _, version := range template.Versions {
			version.AbsPath.SetParentPath(template.Path())
		}
	}

	// Validate
	if err := manifestContent.validate(); err != nil {
		return nil, err
	}

	// Set new version
	manifestContent.Version = build.MajorVersion

	return manifestContent, nil
}

func saveManifestFile(fs filesystem.Fs, manifestContent *file) error {
	// Validate
	err := manifestContent.validate()
	if err != nil {
		return err
	}

	// Write JSON file
	content, err := json.EncodeString(manifestContent, true)
	if err != nil {
		return utils.PrefixError(`cannot encode manifest`, err)
	}
	file := filesystem.NewFile(Path(), content)
	if err := fs.WriteFile(file); err != nil {
		return err
	}

	return nil
}

func (c *file) validate() error {
	if err := validator.Validate(c); err != nil {
		return utils.PrefixError("manifest is not valid", err)
	}
	return nil
}
