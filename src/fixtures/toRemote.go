package fixtures

import (
	"keboola-as-code/src/model"
)

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
