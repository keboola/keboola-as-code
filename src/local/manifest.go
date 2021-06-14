package local

import (
	"fmt"
	"keboola-as-code/src/json"
	"keboola-as-code/src/validator"
	"os"
	"path/filepath"
)

const (
	FileName = "manifest.json"
)

type Manifest struct {
	path           string
	Version        int       `json:"version" validate:"required,min=1,max=1"`
	Project        *Project  `json:"project" validate:"required"`
	Branches       []*Branch `json:"branches"`
	Configurations []*Config `json:"configurations"`
}

type Project struct {
	Id      int    `json:"id" validate:"required,min=1"`
	ApiHost string `json:"apiHost" validate:"required,hostname"`
}

type Branch struct {
	Id   int    `json:"id" validate:"required,min=1"`
	Path string `json:"path" validate:"required"`
}

type Config struct {
	Id          string       `json:"id" validate:"required,min=1"`
	ComponentId string       `json:"componentId" validate:"required"`
	BranchId    int          `json:"branchId" validate:"required"`
	Path        string       `json:"path" validate:"required"`
	Rows        []*ConfigRow `json:"rows"`
}

type ConfigRow struct {
	Id   string `json:"id" validate:"required,min=1"`
	Path string `json:"path" validate:"required"`
}

func NewManifest(projectId int, apiHost string) (*Manifest, error) {
	m := &Manifest{
		Version:        1,
		Project:        &Project{Id: projectId, ApiHost: apiHost},
		Branches:       make([]*Branch, 0),
		Configurations: make([]*Config, 0),
	}
	err := m.Validate()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func LoadManifest(metadataDir string) (*Manifest, error) {
	// Load file
	path := filepath.Join(metadataDir, FileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// Decode JSON
	m := &Manifest{}
	err = json.Decode(data, m)
	if err != nil {
		return nil, err
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
		return fmt.Errorf("Manifest is not valid:\n%s", err)
	}
	return nil
}

func (m *Manifest) Path() string {
	if len(m.path) == 0 {
		panic(fmt.Errorf("path is not set"))
	}
	return m.path
}
