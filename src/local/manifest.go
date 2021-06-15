package local

import (
	"fmt"
	"keboola-as-code/src/json"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"os"
	"path/filepath"
)

const (
	FileName = "manifest.json"
)

type Manifest struct {
	path     string
	Version  int       `json:"version" validate:"required,min=1,max=1"`
	Project  *Project  `json:"project" validate:"required"`
	Branches []*Branch `json:"branches"`
	Configs  []*Config `json:"configurations"`
}

func NewManifest(projectId int, apiHost string) (*Manifest, error) {
	m := &Manifest{
		Version:  1,
		Project:  &Project{Id: projectId, ApiHost: apiHost},
		Branches: make([]*Branch, 0),
		Configs:  make([]*Config, 0),
	}
	err := m.Validate()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func LoadManifest(projectDir string, metadataDir string) (*Manifest, error) {
	// Exists?
	path := filepath.Join(metadataDir, FileName)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("manifest \"%s\" not found", utils.RelPath(projectDir, path))
	}

	// Load file
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read from manifest \"%s\": %s", utils.RelPath(projectDir, path), err)
	}

	// Decode JSON
	m := &Manifest{}
	err = json.Decode(data, m)
	if err != nil {
		return nil, fmt.Errorf("manifest \"%s\" is not valid: %s", utils.RelPath(projectDir, path), err)
	}

	// Validate
	err = m.Validate()
	if err != nil {
		return nil, err
	}

	// Set path
	m.path = path

	// Return
	return m, nil
}

func (m *Manifest) Save(metadataDir string) error {
	// Validate
	err := m.Validate()
	if err != nil {
		return err
	}

	// Encode JSON
	data, err := json.Encode(m, true)
	if err != nil {
		return err
	}

	// Write file
	m.path = filepath.Join(metadataDir, FileName)
	return os.WriteFile(m.path, data, 0650)
}

func (m *Manifest) Validate() error {
	if err := validator.Validate(m); err != nil {
		return fmt.Errorf("manifest is not valid: %s", err)
	}
	return nil
}

func (m *Manifest) Path() string {
	if len(m.path) == 0 {
		panic(fmt.Errorf("manifest path is not set"))
	}
	return m.path
}
