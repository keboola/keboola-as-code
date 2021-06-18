package model

import (
	"fmt"
	"keboola-as-code/src/json"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"os"
	"path/filepath"
)

const (
	MetadataDir      = ".keboola"
	MetaFile         = "meta.json"
	ConfigFile       = "config.json"
	RowsDir          = "rows"
	ManifestFileName = "manifest.json"
)

type Manifest struct {
	path     string
	Version  int               `json:"version" validate:"required,min=1,max=1"`
	Project  *ManifestProject  `json:"project" validate:"required"`
	Branches []*ManifestBranch `json:"branches"`
	Configs  []*ManifestConfig `json:"configurations"`
}
type ManifestProject struct {
	Id      int    `json:"id" validate:"required,min=1"`
	ApiHost string `json:"apiHost" validate:"required,hostname"`
}

type ManifestBranch struct {
	Id   int    `json:"id" validate:"required,min=1"`
	Path string `json:"path" validate:"required"`
}

type BranchMeta struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description" validate:"required"`
	IsDefault   bool   `json:"isDefault"`
}

type ManifestConfig struct {
	BranchId    int                  `json:"branchId" validate:"required"`
	ComponentId string               `json:"componentId" validate:"required"`
	Id          string               `json:"id" validate:"required,min=1"`
	Path        string               `json:"path" validate:"required"`
	Rows        []*ManifestConfigRow `json:"rows"`
}

type ConfigMeta struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description" validate:"required"`
}

type ManifestConfigRow struct {
	Id   string `json:"id" validate:"required,min=1"`
	Path string `json:"path" validate:"required"`
}

type ConfigRowMeta struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description" validate:"required"`
	IsDisabled  bool   `json:"IsDisabled"`
}

func NewManifest(projectId int, apiHost string) (*Manifest, error) {
	m := &Manifest{
		Version:  1,
		Project:  &ManifestProject{Id: projectId, ApiHost: apiHost},
		Branches: make([]*ManifestBranch, 0),
		Configs:  make([]*ManifestConfig, 0),
	}
	err := m.Validate()
	if err != nil {
		return nil, err
	}
	return m, nil
}

func LoadManifest(projectDir string, metadataDir string) (*Manifest, error) {
	// Exists?
	path := filepath.Join(metadataDir, ManifestFileName)
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
	m.path = filepath.Join(metadataDir, ManifestFileName)
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

func (b *ManifestBranch) MetaFilePath(projectDir string) string {
	return filepath.Join(projectDir, b.Path, MetaFile)
}

func (c *ManifestConfig) MetaFilePath(b *ManifestBranch, projectDir string) string {
	return filepath.Join(projectDir, b.Path, c.Path, MetaFile)
}

func (r *ManifestConfigRow) MetaFilePath(b *ManifestBranch, c *ManifestConfig, projectDir string) string {
	return filepath.Join(projectDir, b.Path, c.Path, RowsDir, r.Path, MetaFile)
}

func (c *ManifestConfig) ConfigFilePath(b *ManifestBranch, projectDir string) string {
	return filepath.Join(projectDir, b.Path, c.Path, ConfigFile)
}

func (r *ManifestConfigRow) ConfigFilePath(b *ManifestBranch, c *ManifestConfig, projectDir string) string {
	return filepath.Join(projectDir, b.Path, c.Path, RowsDir, r.Path, ConfigFile)
}

func (b *ManifestBranch) Meta(projectDir string) (*BranchMeta, error) {
	// Read meta file
	path := b.MetaFilePath(projectDir)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("branch metadata JSON file \"%s\" not found", utils.RelPath(projectDir, path))
	}

	meta := &BranchMeta{}
	err := readJsonFile(projectDir, path, meta)
	if err != nil {
		return nil, fmt.Errorf("branch metadata JSON file \"%s\" is invalid: %s", utils.RelPath(projectDir, path), err)
	}
	return meta, err
}

func (c *ManifestConfig) Meta(b *ManifestBranch, projectDir string) (*ConfigMeta, error) {
	path := c.MetaFilePath(b, projectDir)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("config metadata JSON file \"%s\" not found", utils.RelPath(projectDir, path))
	}

	meta := &ConfigMeta{}
	err := readJsonFile(projectDir, path, meta)
	if err != nil {
		return nil, fmt.Errorf("config metadata JSON file \"%s\" is invalid: %s", utils.RelPath(projectDir, path), err)
	}
	return meta, nil
}

func (r *ManifestConfigRow) Meta(b *ManifestBranch, c *ManifestConfig, projectDir string) (*ConfigRowMeta, error) {
	path := r.MetaFilePath(b, c, projectDir)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("config row metadata JSON file \"%s\" not found", utils.RelPath(projectDir, path))
	}

	meta := &ConfigRowMeta{}
	err := readJsonFile(projectDir, path, meta)
	if err != nil {
		return nil, fmt.Errorf("config row metadata JSON file \"%s\" is invalid: %s", utils.RelPath(projectDir, path), err)
	}
	return meta, nil
}

func (c *ManifestConfig) Config(b *ManifestBranch, projectDir string) (map[string]interface{}, error) {
	path := c.ConfigFilePath(b, projectDir)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("config JSON file \"%s\" not found", utils.RelPath(projectDir, path))
	}

	config := make(map[string]interface{})
	err := readJsonFile(projectDir, path, &config)
	if err != nil {
		return nil, fmt.Errorf("config JSON file \"%s\" is invalid: %s", utils.RelPath(projectDir, path), err)
	}
	return config, nil
}

func (r *ManifestConfigRow) Config(b *ManifestBranch, c *ManifestConfig, projectDir string) (map[string]interface{}, error) {
	path := r.ConfigFilePath(b, c, projectDir)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("config row JSON file \"%s\" not found", utils.RelPath(projectDir, path))
	}

	config := make(map[string]interface{})
	err := readJsonFile(projectDir, path, &config)
	if err != nil {
		return nil, fmt.Errorf("config row JSON file \"%s\" is invalid: %s", utils.RelPath(projectDir, path), err)
	}
	return config, nil
}

func (b *ManifestBranch) ToModel(projectDir string) (*Branch, error) {
	// Read meta file
	meta, err := b.Meta(projectDir)
	if err != nil {
		return nil, err
	}

	// Convert
	branch := &Branch{}
	branch.Id = b.Id
	branch.Name = meta.Name
	branch.Description = meta.Description
	branch.IsDefault = meta.IsDefault
	return branch, nil
}

func (c *ManifestConfig) ToModel(b *ManifestBranch, projectDir string) (*Config, error) {
	// Read meta file
	meta, err := c.Meta(b, projectDir)
	if err != nil {
		return nil, err
	}

	// Read config file
	configJson, err := c.Config(b, projectDir)
	if err != nil {
		return nil, err
	}

	// Convert
	config := &Config{}
	config.BranchId = c.BranchId
	config.ComponentId = c.ComponentId
	config.Id = c.Id
	config.Name = meta.Name
	config.Description = meta.Description
	config.Config = configJson

	config.SortRows()
	return config, nil
}

func (r *ManifestConfigRow) ToModel(b *ManifestBranch, c *ManifestConfig, projectDir string) (*ConfigRow, error) {
	// Read meta file
	meta, err := r.Meta(b, c, projectDir)
	if err != nil {
		return nil, err
	}

	// Read config file
	configJson, err := r.Config(b, c, projectDir)
	if err != nil {
		return nil, err
	}

	// Convert
	row := &ConfigRow{}
	row.BranchId = c.BranchId
	row.ComponentId = c.ComponentId
	row.ConfigId = c.Id
	row.Id = r.Id
	row.Name = meta.Name
	row.Description = meta.Description
	row.IsDisabled = meta.IsDisabled
	row.Config = configJson
	return row, nil
}

func readJsonFile(projectDir string, path string, v interface{}) error {
	// Read meta file
	if !utils.IsFile(path) {
		return fmt.Errorf("file not found \"%s\"", utils.RelPath(projectDir, path))
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("cannot read file \"%s\"", utils.RelPath(projectDir, path))
	}

	// Decode meta file
	err = json.Decode(content, v)
	if err != nil {
		return err
	}
	return nil
}
