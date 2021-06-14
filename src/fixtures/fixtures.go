package fixtures

import (
	"fmt"
	"keboola-as-code/src/model/remote"
	"testing"
)

type BranchName string

type Branch struct {
	Name      BranchName `json:"name"`
	IsDefault bool       `json:"isDefault"`
}

type BranchState struct {
	Branch  *Branch   `json:"branch"`
	Configs []*Config `json:"configs"`
}

type BranchStateConfigName struct {
	Branch  *Branch  `json:"branch"`
	Configs []string `json:"configs"`
}

type Config struct {
	ComponentId string                 `json:"componentId"`
	Name        string                 `json:"name"`
	Config      map[string]interface{} `json:"configuration"`
	Rows        []*ConfigRow           `json:"rows"`
}

type ConfigRow struct {
	Name       string                 `json:"name"`
	IsDisabled bool                   `json:"isDisabled"`
	Config     map[string]interface{} `json:"configuration"`
}

type ProjectState struct {
	Branches []*BranchState
}

type StateFile struct {
	AllBranchesConfigs []string                 `json:"allBranchesConfigs"`
	Branches           []*BranchStateConfigName `json:"branches"`
}

// ToRemote maps fixture to remote.Config
func (c *Config) ToRemote() *remote.Config {
	config := &remote.Config{}
	config.ComponentId = c.ComponentId
	config.Name = c.Name
	config.Description = "test fixture"
	config.ChangeDescription = "created by test"
	config.Config = c.Config
	for _, r := range c.Rows {
		row := &remote.ConfigRow{}
		row.Name = r.Name
		row.Description = "test fixture"
		row.ChangeDescription = "created by test"
		row.IsDisabled = r.IsDisabled
		row.Config = r.Config
		config.Rows = append(config.Rows, row)
	}
	return config
}

// ToRemote maps fixture to remote.Branch
func (b *Branch) ToRemote(defaultBranch *remote.Branch) *remote.Branch {
	if b.IsDefault {
		return defaultBranch
	}

	branch := &remote.Branch{}
	branch.Name = string(b.Name)
	branch.Description = "test fixture"
	branch.IsDefault = b.IsDefault
	return branch
}

func ConvertRemoteStateToFixtures(remote *remote.State) *ProjectState {
	fixtures := &ProjectState{}
	branchesByName := make(map[BranchName]*BranchState)

	for _, branch := range remote.Branches() {
		// Map branch
		b := &Branch{}
		b.Name = BranchName(branch.Name)
		b.IsDefault = branch.IsDefault
		bState := &BranchState{Branch: b}
		fixtures.Branches = append(fixtures.Branches, bState)
		branchesByName[b.Name] = bState
	}

	for _, configuration := range remote.Configurations() {
		branchId := configuration.BranchId
		branch, found := remote.BranchById(branchId)
		if !found {
			panic(fmt.Errorf("branch with id \"%d\" not found", branchId))
		}

		// Map configuration
		branchName := BranchName(branch.Name)
		c := &Config{}
		c.ComponentId = configuration.ComponentId
		c.Name = configuration.Name
		c.Config = configuration.Config
		branchesByName[branchName].Configs = append(branchesByName[branchName].Configs, c)

		// Map rows
		for _, row := range configuration.Rows {
			r := &ConfigRow{}
			r.Name = row.Name
			r.Config = row.Config
			c.Rows = append(c.Rows, r)
		}
	}

	return fixtures
}

func SetStateOfTestKbcProject(t *testing.T, projectStateFilePath string) {
	testProject := NewTestProject(t, projectStateFilePath)
	testProject.Clear()
	testProject.InitState()
}
