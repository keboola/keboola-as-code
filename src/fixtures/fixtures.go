package fixtures

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/api"
	"keboola-as-code/src/client"
	"keboola-as-code/src/fixtures/testEnv"
	"keboola-as-code/src/model/remote"
	"keboola-as-code/src/utils"
	"testing"
)

type BranchName string

type Branch struct {
	Name      BranchName `json:"name"`
	IsDefault bool       `json:"isDefault"`
}

type BranchState struct {
	Branch  *Branch   `json:"branch"`
	Configs []*Config `json:"configurations"`
}

type BranchStateConfigName struct {
	Branch  *Branch  `json:"branch"`
	Configs []string `json:"configurations"`
}

type Config struct {
	ComponentId string                 `json:"componentId"`
	Name        string                 `json:"name"`
	Config      map[string]interface{} `json:"configuration"`
	Rows        []*ConfigRow           `json:"rows"`
}

type ConfigRow struct {
	Name   string                 `json:"name"`
	Config map[string]interface{} `json:"configuration"`
}

type ProjectState struct {
	Branches []*BranchState
}

type StateFile struct {
	AllBranches *BranchStateConfigName   `json:"allBranches"`
	Branches    []*BranchStateConfigName `json:"branches"`
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
	a, _ := api.TestStorageApiWithToken(t)
	if testEnv.TestProjectId() != a.ProjectId() {
		assert.FailNow(t, "TEST_PROJECT_ID and token project id are different.")
	}

	// Decode file
	data := utils.GetFileContent(projectStateFilePath)
	stateFile := &StateFile{}
	assert.NoError(t, json.Unmarshal([]byte(data), stateFile))

	// Clear
	clearTestKbcProject(t, a)

	// Create branches and configurations
	//setProjectState(t, a, stateFile)
}

// clearTestKbcProject clears test project using parallel requests pool
func clearTestKbcProject(t *testing.T, a *api.StorageApi) {
	fmt.Printf("Fixtures: Clearing test project \"%s\", id: \"%d\".\n", a.ProjectName(), a.ProjectId())
	pool := a.NewPool()
	pool.
		Request(a.ListBranchesRequest()).
		OnSuccess(func(response *client.Response) *client.Response {
			for _, branch := range *response.Result().(*[]*remote.Branch) {
				if branch.IsDefault {
					// Default branch cannot be deleted, so we have to delete all configurations
					pool.
						Request(a.ListComponentsRequest(branch.Id)).
						OnSuccess(func(response *client.Response) *client.Response {
							for _, component := range *response.Result().(*[]*remote.Component) {
								for _, configuration := range component.Configs {
									// Delete each configuration in branch
									pool.
										Request(a.DeleteConfigRequest(configuration.ComponentId, configuration.Id)).
										Send()
								}
							}
							return response
						}).
						Send()
				} else {
					// Delete dev branch
					pool.
						Request(a.DeleteBranchRequest(branch.Id)).
						Send()
				}
			}
			return response
		}).
		Send()

	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(t, fmt.Sprintf("cannot clear test project: %s", err))
	}
	fmt.Println("Fixtures: Test project cleared.")
}
