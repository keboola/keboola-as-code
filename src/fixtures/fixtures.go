package fixtures

import (
	"keboola-as-code/src/model"
	"testing"
)

func ConvertRemoteStateToFixtures(model *model.State) (*ProjectState, error) {
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

	for _, configuration := range model.Configs() {
		branchId := configuration.BranchId
		branch, err := model.BranchById(branchId)
		if err != nil {
			return nil, err
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

	return fixtures, nil
}

func SetStateOfTestProject(t *testing.T, projectStateFilePath string) {
	testProject := NewTestProject(t, projectStateFilePath)
	testProject.Clear()
	testProject.InitState()
}
