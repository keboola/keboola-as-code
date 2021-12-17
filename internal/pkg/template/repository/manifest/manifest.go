package manifest

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/build"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const FileName = `repository.json`

type Manifest struct {
	fs       filesystem.Fs
	changed  bool
	*Content `validate:"required,dive"` // content of the file, updated only on load/save
}

type Content struct {
	Version int `json:"version" validate:"required,min=1,max=2"`
}

func NewManifest(fs filesystem.Fs) (*Manifest, error) {
	m := newManifest(fs)
	if err := m.validate(); err != nil {
		return nil, err
	}
	return m, nil
}

func newManifest(fs filesystem.Fs) *Manifest {
	return &Manifest{
		fs: fs,
		Content: &Content{
			Version: build.MajorVersion,
		},
	}
}

func Path() string {
	return filesystem.Join(filesystem.MetadataDir, FileName)
}

func (m *Manifest) Path() string {
	return Path()
}

func Load(fs filesystem.Fs, _ *zap.SugaredLogger) (*Manifest, error) {
	// Exists?
	path := filesystem.Join(filesystem.MetadataDir, FileName)
	if !fs.IsFile(path) {
		return nil, fmt.Errorf("manifest \"%s\" not found", path)
	}

	// Read JSON file
	m := newManifest(fs)
	if err := fs.ReadJsonFileTo(path, "manifest", &m.Content); err != nil {
		return nil, err
	}

	// Set new version
	m.Content.Version = build.MajorVersion

	// Validate
	if err := m.validate(); err != nil {
		return nil, err
	}

	// Track if manifest was changed after load
	m.changed = false

	// Return
	return m, nil
}

func (m *Manifest) Save() error {
	// Validate
	err := m.validate()
	if err != nil {
		return err
	}

	// Write JSON file
	content, err := json.EncodeString(m.Content, true)
	if err != nil {
		return utils.PrefixError(`cannot encode manifest`, err)
	}
	file := filesystem.NewFile(m.Path(), content)
	if err := m.fs.WriteFile(file); err != nil {
		return err
	}

	m.changed = false
	return nil
}

func (m *Manifest) validate() error {
	if err := validator.Validate(m); err != nil {
		return utils.PrefixError("manifest is not valid", err)
	}
	return nil
}
