package fixtures

import (
	"fmt"
	"keboola-as-code/src/model/remote"
)

type ProjectState struct {
	Branches       []*Branch
	Configurations []*Configuration
}

type Branch struct {
	Name      string `json:"name"`
	IsDefault bool   `json:"isDefault"`
}

type Component struct {
	BranchName     string           `json:"branchName"`
	Id             string           `json:"id"`
	Type           string           `json:"type"`
	Name           string           `json:"name"`
	Configurations []*Configuration `json:"configurations"`
}

type Configuration struct {
	BranchName    string                 `json:"branchName"`
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
	fixtures := &ProjectState{}

	for _, branch := range remote.Branches() {
		// Map branch
		b := &Branch{}
		b.Name = branch.Name
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
		c := &Configuration{}
		c.BranchName = branch.Name
		c.ComponentId = configuration.ComponentId
		c.Name = configuration.Name
		c.Configuration = configuration.Configuration
		fixtures.Configurations = append(fixtures.Configurations, c)

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
