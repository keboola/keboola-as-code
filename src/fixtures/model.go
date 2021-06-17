package fixtures

import (
	"encoding/json"
	"fmt"
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
	branchesByName := make(map[string]*BranchState)

	for _, branch := range model.Branches() {
		// Map branch
		b := &Branch{}
		b.Name = branch.Name
		b.IsDefault = branch.IsDefault
		bState := &BranchState{Branch: b}
		fixtures.Branches = append(fixtures.Branches, bState)
		branchesByName[b.Name] = bState
	}

	for _, configuration := range model.Configs() {
		branchId := configuration.BranchId
		branch, err := model.BranchById(branchId)
		if err != nil {
			return nil, err
		}

		// Map configuration
		branchName := branch.Name
		c := &Config{}
		c.ComponentId = configuration.ComponentId
		c.Name = configuration.Name
		c.Config = configuration.Config
		branchesByName[branchName].Configs = append(branchesByName[branchName].Configs, c)

		// Map rows
		for _, row := range configuration.Rows {
			r := &ConfigRow{}
			r.Name = row.Name
			r.IsDisabled = row.IsDisabled
			r.Config = row.Config
			c.Rows = append(c.Rows, r)
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
