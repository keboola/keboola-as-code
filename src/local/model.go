package local

import (
	"fmt"
	"keboola-as-code/src/json"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"os"
	"path/filepath"
)

const (
	MetaFile   = "meta.json"
	ConfigFile = "config.json"
	RowsDir    = "rows"
)

type Project struct {
	Id      int    `json:"id" validate:"required,min=1"`
	ApiHost string `json:"apiHost" validate:"required,hostname"`
}

type Branch struct {
	Id   int    `json:"id" validate:"required,min=1"`
	Path string `json:"path" validate:"required"`
}

type BranchMeta struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description" validate:"required"`
	IsDefault   bool   `json:"isDefault"`
}

type Config struct {
	BranchId    int          `json:"branchId" validate:"required"`
	ComponentId string       `json:"componentId" validate:"required"`
	Id          string       `json:"id" validate:"required,min=1"`
	Path        string       `json:"path" validate:"required"`
	Rows        []*ConfigRow `json:"rows"`
}

type ConfigMeta struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description" validate:"required"`
}

type ConfigRow struct {
	Id   string `json:"id" validate:"required,min=1"`
	Path string `json:"path" validate:"required"`
}

type ConfigRowMeta struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description" validate:"required"`
	IsDisabled  bool   `json:"IsDisabled"`
}

func (b *Branch) MetaFilePath(projectDir string) string {
	return filepath.Join(projectDir, b.Path, MetaFile)
}

func (c *Config) MetaFilePath(b *Branch, projectDir string) string {
	return filepath.Join(projectDir, b.Path, c.Path, MetaFile)
}

func (r *ConfigRow) MetaFilePath(b *Branch, c *Config, projectDir string) string {
	return filepath.Join(projectDir, b.Path, c.Path, RowsDir, r.Path, MetaFile)
}

func (c *Config) ConfigFilePath(b *Branch, projectDir string) string {
	return filepath.Join(projectDir, b.Path, c.Path, ConfigFile)
}

func (r *ConfigRow) ConfigFilePath(b *Branch, c *Config, projectDir string) string {
	return filepath.Join(projectDir, b.Path, c.Path, RowsDir, r.Path, ConfigFile)
}

func (b *Branch) Meta(projectDir string) (*BranchMeta, error) {
	// Read meta file
	path := b.MetaFilePath(projectDir)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("branch metadata file not found \"%s\"", utils.RelPath(projectDir, path))
	}

	meta := &BranchMeta{}
	err := readJsonFile(projectDir, path, meta)
	if err != nil {
		return nil, fmt.Errorf("branch metadata file is invalid, %s", err)
	}
	return meta, err
}

func (c *Config) Meta(b *Branch, projectDir string) (*ConfigMeta, error) {
	path := c.MetaFilePath(b, projectDir)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("config metadata file not found \"%s\"", utils.RelPath(projectDir, path))
	}

	meta := &ConfigMeta{}
	err := readJsonFile(projectDir, path, meta)
	if err != nil {
		return nil, fmt.Errorf("config metadata file is invalid, %s", err)
	}
	return meta, nil
}

func (r *ConfigRow) Meta(b *Branch, c *Config, projectDir string) (*ConfigRowMeta, error) {
	path := r.MetaFilePath(b, c, projectDir)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("config row metadata file not found \"%s\"", utils.RelPath(projectDir, path))
	}

	meta := &ConfigRowMeta{}
	err := readJsonFile(projectDir, path, meta)
	if err != nil {
		return nil, fmt.Errorf("config row metadata file is invalid, %s", err)
	}
	return meta, nil
}

func (c *Config) Config(b *Branch, projectDir string) (map[string]interface{}, error) {
	path := c.ConfigFilePath(b, projectDir)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("config content file not found \"%s\"", utils.RelPath(projectDir, path))
	}

	config := make(map[string]interface{})
	err := readJsonFile(projectDir, path, &config)
	if err != nil {
		return nil, fmt.Errorf("config content  is invalid, %s", err)
	}
	return config, nil
}

func (r *ConfigRow) Config(b *Branch, c *Config, projectDir string) (map[string]interface{}, error) {
	path := r.ConfigFilePath(b, c, projectDir)
	if !utils.IsFile(path) {
		return nil, fmt.Errorf("config row content file not found \"%s\"", utils.RelPath(projectDir, path))
	}

	config := make(map[string]interface{})
	err := readJsonFile(projectDir, path, &config)
	if err != nil {
		return nil, fmt.Errorf("config row content is invalid, %s", err)
	}
	return config, nil
}

func (b *Branch) ToModel(projectDir string) (*model.Branch, error) {
	// Read meta file
	meta, err := b.Meta(projectDir)
	if err != nil {
		return nil, err
	}

	// Convert
	branch := &model.Branch{}
	branch.Id = b.Id
	branch.Name = meta.Name
	branch.Description = meta.Description
	branch.IsDefault = meta.IsDefault
	return branch, nil
}

func (c *Config) ToModel(b *Branch, projectDir string) (*model.Config, error) {
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
	config := &model.Config{}
	config.BranchId = c.BranchId
	config.ComponentId = c.ComponentId
	config.Id = c.Id
	config.Name = meta.Name
	config.Description = meta.Description
	config.Config = configJson

	// Rows
	for _, r := range c.Rows {
		row, err := r.ToModel(b, c, projectDir)
		if err != nil {
			return nil, err
		}
		config.Rows = append(config.Rows, row)
	}

	return config, nil
}

func (r *ConfigRow) ToModel(b *Branch, c *Config, projectDir string) (*model.ConfigRow, error) {
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
	row := &model.ConfigRow{}
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
		return fmt.Errorf("invalid JSON file \"%s\":\n%s", utils.RelPath(projectDir, path), err)
	}
	return nil
}
