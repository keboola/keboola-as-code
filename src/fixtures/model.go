package fixtures

import (
	"encoding/json"
	"fmt"
	"github.com/iancoleman/orderedmap"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"path/filepath"
	"runtime"
	"testing"
)

type Branch struct {
	Name        string `json:"name" validate:"required"`
	Description string `json:"description" validate:"required"`
	IsDefault   bool   `json:"isDefault"`
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
	Config      *orderedmap.OrderedMap `json:"configuration"`
	Rows        []*ConfigRow           `json:"rows"`
}

type ConfigRow struct {
	Name       string                 `json:"name" validate:"required"`
	IsDisabled bool                   `json:"isDisabled"`
	Config     *orderedmap.OrderedMap `json:"configuration"`
}

type ProjectState struct {
	Branches []*BranchState
}

type StateFile struct {
	AllBranchesConfigs []string                 `json:"allBranchesConfigs" validate:"required"`
	Branches           []*BranchStateConfigName `json:"branches" validate:"required"`
}

func (p *ProjectState) BranchStateByName(name string) *BranchState {
	for _, f := range p.Branches {
		if f.Branch.Name == name {
			return f
		}
	}
	panic(fmt.Errorf("cannot find branch fixture \"%s\"", name))
}

func BranchFromModel(b *model.Branch) *Branch {
	f := &Branch{}
	f.Name = b.Name
	f.IsDefault = b.IsDefault
	return f
}

func ConfigFromModel(c *model.Config) *Config {
	f := &Config{}
	f.ComponentId = c.ComponentId
	f.Name = c.Name
	f.Config = c.Config
	return f
}

func ConfigRowFromModel(r *model.ConfigRow) *ConfigRow {
	f := &ConfigRow{}
	f.Name = r.Name
	f.IsDisabled = r.IsDisabled
	f.Config = r.Config
	return f
}

// ToModel maps fixture to model.Branch
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
		stateFile.Branches = append(stateFile.Branches, &BranchStateConfigName{
			Branch: &Branch{Name: "Main", IsDefault: true},
		})
	}

	return stateFile, nil
}

func ConvertRemoteStateToFixtures(model *model.State) (*ProjectState, error) {
	fixtures := &ProjectState{}
	branchesById := make(map[int]*BranchState)

	for _, s := range model.Branches() {
		branch := s.Remote
		if branch == nil {
			continue
		}
		state := &BranchState{Branch: BranchFromModel(branch)}
		branchesById[branch.Id] = state
		fixtures.Branches = append(fixtures.Branches, state)
	}

	for _, s := range model.Configs() {
		config := s.Remote
		if config == nil {
			continue
		}

		// Map config
		c := ConfigFromModel(config)
		branch := branchesById[config.BranchId]
		branchState := fixtures.BranchStateByName(branch.Branch.Name)
		branchState.Configs = append(branchState.Configs, c)

		// Map rows
		for _, row := range config.Rows {
			c.Rows = append(c.Rows, ConfigRowFromModel(row))
		}
	}

	return fixtures, nil
}

// LoadConfig loads config from the JSON file
func LoadConfig(t *testing.T, name string) *model.Config {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	path := filepath.Join(testDir, "configs", name+".json")
	content := utils.GetFileContent(path)
	fixture := &Config{}
	err := json.Unmarshal([]byte(content), fixture)
	if err != nil {
		assert.FailNowf(t, "cannot decode JSON file \"%s\": %s", path, err)
	}
	return fixture.ToModel()
}
