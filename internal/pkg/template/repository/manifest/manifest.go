package manifest

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Manifest struct {
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

func New() *Manifest {
	return &Manifest{
		templates: make([]*TemplateRecord, 0),
	}
}

func (m *Manifest) Path() string {
	return Path()
}

func Load(fs filesystem.Fs) (*Manifest, error) {
	// Load file content
	manifestContent, err := loadFile(fs)
	if err != nil {
		return nil, err
	}

	// Create manifest
	m := New()
	m.templates = manifestContent.Templates

	// Track if manifest was changed after load
	m.changed = false

	// Return
	return m, nil
}

func (m *Manifest) Save(fs filesystem.Fs) error {
	// Create file content
	content := newFile()
	content.Templates = m.templates

	// Save file
	if err := saveFile(fs, content); err != nil {
		return err
	}

	m.changed = false
	return nil
}
