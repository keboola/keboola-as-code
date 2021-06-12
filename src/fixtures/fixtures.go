package fixtures

import (
	"fmt"
	"keboola-as-code/src/model/remote"
)

type BranchName string

type ProjectState struct {
	Branches       []*Branch
	Configurations map[BranchName][]*Configuration `json:"configurations"`
}

type Branch struct {
	Name      BranchName `json:"name"`
	IsDefault bool       `json:"isDefault"`
}

type Configuration struct {
	ComponentId   string                 `json:"componentId"`
	Name          string                 `json:"name"`
	Configuration map[string]interface{} `json:"configuration"`
	Rows          []*Row                 `json:"rows"`
}

type Row struct {
	Name          string                 `json:"name"`
	Configuration map[string]interface{} `json:"configuration"`
}

func ConvertRemoteStateToFixtures(remote *remote.State) *ProjectState {
	fixtures := &ProjectState{
		Configurations: make(map[BranchName][]*Configuration),
	}

	for _, branch := range remote.Branches() {
		// Map branch
		b := &Branch{}
		b.Name = BranchName(branch.Name)
		b.IsDefault = branch.IsDefault
		fixtures.Branches = append(fixtures.Branches, b)
	}

	for _, configuration := range remote.Configurations() {
		branchId := configuration.BranchId
		branch, found := remote.BranchById(branchId)
		if !found {
			panic(fmt.Errorf("branch with id \"%d\" not found", branchId))
		}

		// Map configuration
		branchName := BranchName(branch.Name)
		c := &Configuration{}
		c.ComponentId = configuration.ComponentId
		c.Name = configuration.Name
		c.Configuration = configuration.Configuration
		fixtures.Configurations[branchName] = append(fixtures.Configurations[branchName], c)

		// Map rows
		for _, row := range configuration.Rows {
			r := &Row{}
			r.Name = row.Name
			r.Configuration = row.Configuration
			c.Rows = append(c.Rows, r)
		}
	}

	return fixtures
}
