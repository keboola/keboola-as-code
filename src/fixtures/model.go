package fixtures

import (
	"keboola-as-code/src/model"
)

type BranchName string

type Branch struct {
	Name      BranchName `json:"name" validate:"required"`
	IsDefault bool       `json:"isDefault"`
}

type BranchState struct {
	Branch  *Branch   `json:"branch" validate:"required"`
	Configs []*Config `json:"configs"`
}

type BranchStateConfigName struct {
	Branch  *Branch  `json:"branch" validate:"required"`
	Configs []string `json:"configs"`
}

type Config struct {
	ComponentId string                 `json:"componentId" validate:"required"`
	Name        string                 `json:"name" validate:"required"`
	Config      map[string]interface{} `json:"configuration"`
	Rows        []*ConfigRow           `json:"rows"`
}

type ConfigRow struct {
	Name       string                 `json:"name" validate:"required"`
	IsDisabled bool                   `json:"isDisabled"`
	Config     map[string]interface{} `json:"configuration"`
}

type ProjectState struct {
	Branches []*BranchState
}

type StateFile struct {
	AllBranchesConfigs []string                 `json:"allBranchesConfigs" validate:"required"`
	Branches           []*BranchStateConfigName `json:"branches" validate:"required"`
}

// ToModel maps fixture to model.Branch
func (b *Branch) ToModel(defaultBranch *model.Branch) *model.Branch {
	if b.IsDefault {
		return defaultBranch
	}

	branch := &model.Branch{}
	branch.Name = string(b.Name)
	branch.Description = "test fixture"
	branch.IsDefault = b.IsDefault
	return branch
}

// ToModel maps fixture to model.Config
func (c *Config) ToModel() *model.Config {
	config := &model.Config{}
	config.ComponentId = c.ComponentId
	config.Name = c.Name
	config.Description = "test fixture"
	config.ChangeDescription = "created by test"
	config.Config = c.Config
	for _, r := range c.Rows {
		config.Rows = append(config.Rows, r.ToModel())
	}
	return config
}

// ToModel maps fixture to model.Config
func (r *ConfigRow) ToModel() *model.ConfigRow {
	row := &model.ConfigRow{}
	row.Name = r.Name
	row.Description = "test fixture"
	row.ChangeDescription = "created by test"
	row.IsDisabled = r.IsDisabled
	row.Config = r.Config
	return row
}
