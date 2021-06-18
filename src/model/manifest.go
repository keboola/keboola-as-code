package model

import (
	"fmt"
	"github.com/iancoleman/strcase"
	"keboola-as-code/src/json"
	"keboola-as-code/src/utils"
	"keboola-as-code/src/validator"
	"os"
	"path/filepath"
	"regexp"
)

const (
	MetadataDir      = ".keboola"
	MetaFile         = "meta.json"
	ConfigFile       = "config.json"
	RowsDir          = "rows"
	ManifestFileName = "manifest.json"
)

type Manifest struct {
	Path     string            `json:"-"`
	Version  int               `json:"version" validate:"required,min=1,max=1"`
	Project  *ProjectManifest  `json:"project" validate:"required"`
	Branches []*BranchManifest `json:"branches"`
	Configs  []*ConfigManifest `json:"configurations"`
}
type ProjectManifest struct {
	Id      int    `json:"id" validate:"required,min=1"`
	ApiHost string `json:"apiHost" validate:"required,hostname"`
}

type BranchManifest struct {
	Id           int    `json:"id" validate:"required,min=1"`
	Path         string `json:"path" validate:"required"`
	RelativePath string `json:"-" validate:"required"` // generated, not in JSON
	MetadataFile string `json:"-" validate:"required"` // generated, not in JSON
}

type BranchMetadata struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description" validate:"required"`
	IsDefault   bool   `json:"isDefault"`
}

type ConfigManifest struct {
	BranchId     int                  `json:"branchId" validate:"required"`
	ComponentId  string               `json:"componentId" validate:"required"`
	Id           string               `json:"id" validate:"required,min=1"`
	Path         string               `json:"path" validate:"required"`
	Rows         []*ConfigRowManifest `json:"rows"`
	RelativePath string               `json:"-" validate:"required"` // generated, not in JSON
	MetadataFile string               `json:"-" validate:"required"` // generated, not in JSON
	ConfigFile   string               `json:"-" validate:"required"` // generated, not in JSON
}

type ConfigMetadata struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description" validate:"required"`
}

type ConfigRowManifest struct {
	Id           string `json:"id" validate:"required,min=1"`
	Path         string `json:"path" validate:"required"`
	BranchId     int    `json:"-" validate:"required"`
	ComponentId  string `json:"-" validate:"required"` // generated, not in JSON
	ConfigId     string `json:"-" validate:"required"` // generated, not in JSON
	RelativePath string `json:"-" validate:"required"` // generated, not in JSON
	MetadataFile string `json:"-" validate:"required"` // generated, not in JSON
	ConfigFile   string `json:"-" validate:"required"` // generated, not in JSON
}

type ConfigRowMetadata struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description" validate:"required"`
	IsDisabled  bool   `json:"IsDisabled"`
}

func NewManifest(projectId int, apiHost string) (*Manifest, error) {
	m := &Manifest{
		Version:  1,
		Project:  &ProjectManifest{Id: projectId, ApiHost: apiHost},
		Branches: make([]*BranchManifest, 0),
		Configs:  make([]*ConfigManifest, 0),
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

	// Resolve paths
	branchById := make(map[int]*BranchManifest)
	for _, branch := range m.Branches {
		branch.Resolve()
		branchById[branch.Id] = branch
	}
	for _, config := range m.Configs {
		branch, found := branchById[config.BranchId]
		if !found {
			return nil, fmt.Errorf("branch \"%d\" not found in manifest - referenced from the config \"%s:%s\" in \"%s\"", config.BranchId, config.ComponentId, config.Id, m.Path)
		}
		config.Resolve(branch)
		for _, row := range config.Rows {
			row.Resolve(config)
		}
	}

	// Set path
	m.Path = path

	// Validate
	err = m.Validate()
	if err != nil {
		return nil, err
	}

	// Return
	return m, nil
}

func (m *Manifest) Save(metadataDir string) error {
	// Set path
	m.Path = filepath.Join(metadataDir, ManifestFileName)

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
	return os.WriteFile(m.Path, data, 0650)
}

func (m *Manifest) Validate() error {
	if err := validator.Validate(m); err != nil {
		return fmt.Errorf("manifest is not valid: %s", err)
	}
	return nil
}

func (b *BranchManifest) Metadata(projectDir string) (*BranchMetadata, error) {
	meta := &BranchMetadata{}
	if err := readMetadataFile("branch", projectDir, b.MetadataFile, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (c *ConfigManifest) Metadata(projectDir string) (*ConfigMetadata, error) {
	meta := &ConfigMetadata{}
	if err := readMetadataFile("config", projectDir, c.MetadataFile, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (r *ConfigRowManifest) Metadata(projectDir string) (*ConfigRowMetadata, error) {
	meta := &ConfigRowMetadata{}
	if err := readMetadataFile("config row", projectDir, r.MetadataFile, meta); err != nil {
		return nil, err
	}
	return meta, nil
}

func (c *ConfigManifest) ConfigContent(projectDir string) (map[string]interface{}, error) {
	config := make(map[string]interface{})
	if err := readConfigFile("config", projectDir, c.ConfigFile, &config); err != nil {
		return nil, err
	}
	return config, nil
}

func (r *ConfigRowManifest) ConfigContent(projectDir string) (map[string]interface{}, error) {
	config := make(map[string]interface{})
	if err := readConfigFile("config row", projectDir, r.ConfigFile, &config); err != nil {
		return nil, err
	}
	return config, nil
}

func (b *BranchManifest) Resolve() {
	b.RelativePath = b.Path
	b.MetadataFile = filepath.Join(b.RelativePath, MetaFile)
}

func (c *ConfigManifest) Resolve(b *BranchManifest) {
	c.RelativePath = filepath.Join(b.RelativePath, c.Path)
	c.MetadataFile = filepath.Join(c.RelativePath, MetaFile)
	c.ConfigFile = filepath.Join(c.RelativePath, ConfigFile)
}

func (r *ConfigRowManifest) Resolve(c *ConfigManifest) {
	r.BranchId = c.BranchId
	r.ComponentId = c.ComponentId
	r.ConfigId = c.Id
	r.RelativePath = filepath.Join(c.RelativePath, RowsDir, r.Path)
	r.MetadataFile = filepath.Join(r.RelativePath, MetaFile)
	r.ConfigFile = filepath.Join(r.RelativePath, ConfigFile)
}

func (b *BranchManifest) ToModel(projectDir string) (*Branch, error) {
	// Read metadata file
	metadata, err := b.Metadata(projectDir)
	if err != nil {
		return nil, err
	}

	// Convert
	branch := &Branch{}
	branch.Id = b.Id
	branch.Name = metadata.Name
	branch.Description = metadata.Description
	branch.IsDefault = metadata.IsDefault
	return branch, nil
}

func (c *ConfigManifest) ToModel(projectDir string) (*Config, error) {
	// Read metadata file
	metadata, err := c.Metadata(projectDir)
	if err != nil {
		return nil, err
	}

	// Read config file
	content, err := c.ConfigContent(projectDir)
	if err != nil {
		return nil, err
	}

	// Convert
	config := &Config{}
	config.BranchId = c.BranchId
	config.ComponentId = c.ComponentId
	config.Id = c.Id
	config.Name = metadata.Name
	config.Description = metadata.Description
	config.Config = content
	config.Rows = make([]*ConfigRow, 0)

	return config, nil
}

func (r *ConfigRowManifest) ToModel(projectDir string) (*ConfigRow, error) {
	// Read metadata file
	metadata, err := r.Metadata(projectDir)
	if err != nil {
		return nil, err
	}

	// Read config file
	content, err := r.ConfigContent(projectDir)
	if err != nil {
		return nil, err
	}

	// Convert
	row := &ConfigRow{}
	row.BranchId = r.BranchId
	row.ComponentId = r.ComponentId
	row.ConfigId = r.ConfigId
	row.Id = r.Id
	row.Name = metadata.Name
	row.Description = metadata.Description
	row.IsDisabled = metadata.IsDisabled
	row.Config = content
	return row, nil
}

func (b *Branch) GenerateManifest() *BranchManifest {
	manifest := &BranchManifest{}
	manifest.Id = b.Id
	manifest.Path = nameToPath(b.Name)
	manifest.Resolve()
	return manifest
}

func (c *Config) GenerateManifest(b *BranchManifest) *ConfigManifest {
	manifest := &ConfigManifest{}
	manifest.Id = c.Id
	manifest.Path = nameToPath(c.Name)
	manifest.Resolve(b)
	return manifest
}

func (r *ConfigRow) GenerateManifest(c *ConfigManifest) *ConfigRowManifest {
	manifest := &ConfigRowManifest{}
	manifest.Id = r.Id
	manifest.Path = nameToPath(r.Name)
	manifest.Resolve(c)
	return manifest
}

func readConfigFile(kind, projectDir, relPath string, v interface{}) error {
	path := filepath.Join(projectDir, relPath)
	if !utils.IsFile(path) {
		return fmt.Errorf("%s JSON file \"%s\" not found", kind, relPath)
	}

	if err := readJsonFile(projectDir, path, v); err != nil {
		return fmt.Errorf("%s JSON file \"%s\" is invalid: %s", kind, relPath, err)
	}
	return nil
}

func readMetadataFile(kind, projectDir, relPath string, v interface{}) error {
	path := filepath.Join(projectDir, relPath)
	if !utils.IsFile(path) {
		return fmt.Errorf("%s metadata JSON file \"%s\" not found", kind, relPath)
	}

	if err := readJsonFile(projectDir, path, v); err != nil {
		return fmt.Errorf("%s metadata JSON file \"%s\" is invalid: %s", kind, relPath, err)
	}
	return nil
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

func nameToPath(name string) string {
	return regexp.MustCompile(`[^a-zA-Z0-9-]]`).ReplaceAllString(strcase.ToDelimited(name, '-'), "-")
}
