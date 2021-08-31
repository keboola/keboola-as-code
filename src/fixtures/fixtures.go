package fixtures

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/iancoleman/orderedmap"
	"github.com/stretchr/testify/assert"

	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
)

type ProjectSnapshot struct {
	Branches []*BranchConfigs `json:"branches"`
}

type Branch struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description"`
	IsDefault   bool   `json:"isDefault"`
}

type BranchState struct {
	Branch  *Branch  `json:"branch" validate:"required"`
	Configs []string `json:"configs"`
}

type BranchConfigs struct {
	Branch  *Branch   `json:"branch" validate:"required"`
	Configs []*Config `json:"configs"`
}

type Config struct {
	ComponentId       string                 `json:"componentId" validate:"required"`
	Name              string                 `json:"name" validate:"required"`
	Description       string                 `json:"description"`
	ChangeDescription string                 `json:"changeDescription"`
	Content           *orderedmap.OrderedMap `json:"configuration"`
	Rows              []*ConfigRow           `json:"rows"`
}

type ConfigRow struct {
	Name              string                 `json:"name" validate:"required"`
	Description       string                 `json:"description"`
	ChangeDescription string                 `json:"changeDescription"`
	IsDisabled        bool                   `json:"isDisabled"`
	Content           *orderedmap.OrderedMap `json:"configuration"`
}

type StateFile struct {
	AllBranchesConfigs []string       `json:"allBranchesConfigs" validate:"required"`
	Branches           []*BranchState `json:"branches" validate:"required"`
}

// ToModel maps fixture to model.Branch.
func (b *Branch) ToModel(defaultBranch *model.Branch) *model.Branch {
	if b.IsDefault {
		return defaultBranch
	}

	branch := &model.Branch{}
	branch.Name = b.Name
	branch.Description = b.Description
	branch.IsDefault = b.IsDefault
	return branch
}

// ToModel maps fixture to model.Config.
func (c *Config) ToModel() *model.ConfigWithRows {
	config := &model.ConfigWithRows{Config: &model.Config{}}
	config.ComponentId = c.ComponentId
	config.Name = c.Name
	config.Description = "test fixture"
	config.ChangeDescription = "created by test"
	config.Content = c.Content

	for _, r := range c.Rows {
		row := r.ToModel()
		config.Rows = append(config.Rows, row)
	}

	return config
}

// ToModel maps fixture to model.Config.
func (r *ConfigRow) ToModel() *model.ConfigRow {
	row := &model.ConfigRow{}
	row.Name = r.Name
	row.Description = "test fixture"
	row.ChangeDescription = "created by test"
	row.IsDisabled = r.IsDisabled
	row.Content = r.Content
	return row
}

func (b *BranchConfigs) GetName() string {
	return b.Branch.Name
}

func (c *Config) GetName() string {
	return c.Name
}

func (r *ConfigRow) GetName() string {
	return r.Name
}

func LoadStateFile(path string) (*StateFile, error) {
	data := utils.GetFileContent(path)
	stateFile := &StateFile{}
	err := json.Unmarshal([]byte(data), stateFile)
	if err != nil {
		return nil, fmt.Errorf("cannot parse project state file \"%s\": %s", path, err)
	}

	// Check if main branch defined
	// Create definition if not exists
	found := false
	for _, branch := range stateFile.Branches {
		if branch.Branch.IsDefault {
			found = true
			break
		}
	}
	if !found {
		stateFile.Branches = append(stateFile.Branches, &BranchState{
			Branch: &Branch{Name: "Main", IsDefault: true},
		})
	}

	return stateFile, nil
}

// LoadConfig loads config from the file.
func LoadConfig(t *testing.T, name string) *model.ConfigWithRows {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	path := filepath.Join(testDir, "configs", name+".json")
	content := utils.GetFileContent(path)
	fixture := &Config{}
	err := json.Unmarshal([]byte(content), fixture)
	if err != nil {
		assert.FailNowf(t, "cannot decode file \"%s\": %s", path, err)
	}
	return fixture.ToModel()
}
