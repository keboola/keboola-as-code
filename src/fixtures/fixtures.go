package fixtures

import (
	"fmt"
	"keboola-as-code/src/model"
	"testing"
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

func ConvertRemoteStateToFixtures(model *model.State) *ProjectState {
	fixtures := &ProjectState{}
	branchesByName := make(map[BranchName]*BranchState)

	for _, branch := range model.Branches() {
		// Map branch
		b := &Branch{}
		b.Name = BranchName(branch.Name)
		b.IsDefault = branch.IsDefault
		bState := &BranchState{Branch: b}
		fixtures.Branches = append(fixtures.Branches, bState)
		branchesByName[b.Name] = bState
	}

	for _, configuration := range model.Configurations() {
		branchId := configuration.BranchId
		branch, found := model.BranchById(branchId)
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

func SetStateOfTestProject(t *testing.T, projectStateFilePath string) {
	testProject := NewTestProject(t, projectStateFilePath)
	testProject.Clear()
	testProject.InitState()
}
