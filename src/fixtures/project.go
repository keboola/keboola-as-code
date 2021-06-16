package fixtures

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"keboola-as-code/src/api"
	"keboola-as-code/src/client"
	"keboola-as-code/src/fixtures/testEnv"
	"keboola-as-code/src/model"
	"keboola-as-code/src/utils"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

type TestProject struct {
	t              *testing.T
	testDir        string
	stateFilePath  string
	stateFile      *StateFile
	api            *api.StorageApi
	logs           *utils.Writer
	defaultBranch  *model.Branch
	branchesByName map[BranchName]*model.Branch
}

func NewTestProject(t *testing.T, stateFilePath string) *TestProject {
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filepath.Dir(testFile)
	p := &TestProject{t: t, testDir: testDir, stateFilePath: stateFilePath, branchesByName: make(map[BranchName]*model.Branch)}
	p.createApi()
	p.loadDefaultBranch()
	p.loadStateFile()
	p.log("Initializing test project \"%s\", id: \"%d\".", p.api.ProjectName(), p.api.ProjectId())
	return p
}

// Clear deletes all project branches (except default) and all configurations
func (p *TestProject) Clear() {
	startTime := time.Now()
	p.log("Clearing project ...")
	branches := p.loadBranches()
	pool := p.api.NewPool()
	for _, branch := range branches {
		if branch.IsDefault {
			// Default branch cannot be deleted, so we have to delete all configurations
			p.DeleteAllConfigs(pool, branch)
		} else {
			p.DeleteBranch(pool, branch)
		}
	}
	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot delete branches: %s", err))
	}
	p.log("Test project cleared | %s", time.Since(startTime))
}

// InitState crates branches and configurations according stateFile
func (p *TestProject) InitState() {
	startTime := time.Now()
	p.log("Setting project state ...")
	p.CreateConfigsInDefaultBranch()
	p.CreateBranches()
	p.CreateConfigsInBranches()
	p.log("Project state set | %s", time.Since(startTime))
}

func (p *TestProject) DeleteBranch(pool *client.Pool, branch *model.Branch) *client.Request {
	if branch.IsDefault {
		panic(fmt.Errorf("default branch cannot be deleted"))
	}
	return p.api.DeleteBranchRequest(branch.Id).Send()
}

func (p *TestProject) DeleteAllConfigs(pool *client.Pool, branch *model.Branch) {
	if !branch.IsDefault {
		panic(fmt.Errorf("only configs from default branch can be deleted"))
	}

	pool.
		Request(p.api.ListComponentsRequest(branch.Id)).
		OnSuccess(func(response *client.Response) *client.Response {
			for _, component := range *response.Result().(*[]*model.Component) {
				for _, config := range component.Configs {
					// Delete each configuration in branch
					p.DeleteConfig(pool, config.ComponentId, config.Id)
				}
			}
			return response
		}).
		Send()
}

func (p *TestProject) DeleteConfig(pool *client.Pool, componentId string, configId string) *client.Request {
	return pool.
		Request(p.api.DeleteConfigRequest(componentId, configId)).
		Send()
}

func (p *TestProject) CreateBranches() {
	// Create branches sequentially, parallel requests don't work good with this endpoint
	for _, branch := range p.stateFile.Branches {
		if branch.Branch.IsDefault {
			// Default branch already exists
			continue
		}
		modelBranch := p.createBranch(branch.Branch, p.defaultBranch)
		p.branchesByName[BranchName(modelBranch.Name)] = modelBranch
	}
}

func (p *TestProject) CreateConfigsInBranch(pool *client.Pool, names []string, branch *model.Branch) {
	for _, name := range names {
		config := p.getConfigFixture(name)
		config.BranchId = branch.Id

		p.log("creating config \"%s/%s/%s\"", branch.Name, config.ComponentId, config.Name)
		request, err := p.api.CreateConfigRequest(config)
		if err != nil {
			assert.FailNow(p.t, fmt.Sprintf("cannot create create config request: %s", err))
		}
		pool.Request(request).Send()
	}
}

// CreateConfigsInDefaultBranch creates configurations in default branch
// -> before creating other branches
// -> so they will be present in all branches
func (p *TestProject) CreateConfigsInDefaultBranch() {
	pool := p.api.NewPool()
	p.CreateConfigsInBranch(pool, p.stateFile.AllBranchesConfigs, p.defaultBranch)
	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot create configurations in main branch: %s", err))
	}
}

func (p *TestProject) CreateConfigsInBranches() {
	pool := p.api.NewPool()
	for _, branch := range p.stateFile.Branches {
		modelBranch := p.branchesByName[branch.Branch.Name]
		p.CreateConfigsInBranch(pool, branch.Configs, modelBranch)
	}
	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot create configurations: %s", err))
	}
}

func (p *TestProject) log(format string, a ...interface{}) {
	a = append([]interface{}{p.t.Name()}, a...)
	fmt.Printf("Fixtures[%s]: "+format+"\n", a...)
}

func (p *TestProject) loadBranches() []*model.Branch {
	branches, err := p.api.ListBranches()
	if err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot load branches: %s", err))
	}
	return *branches
}

func (p *TestProject) createApi() {
	p.api, p.logs = api.TestStorageApiWithToken(p.t)
	if testEnv.TestProjectId() != p.api.ProjectId() {
		assert.FailNow(p.t, "TEST_PROJECT_ID and token project id are different.")
	}
}

func (p *TestProject) loadDefaultBranch() {
	defaultBranch, err := p.api.GetDefaultBranch()
	if err != nil {
		assert.FailNow(p.t, "cannot get default branch")
	}
	p.defaultBranch = defaultBranch
}

func (p *TestProject) loadStateFile() {
	data := utils.GetFileContent(p.stateFilePath)
	p.stateFile = &StateFile{}
	err := json.Unmarshal([]byte(data), p.stateFile)
	if err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot parse project state file \"%s\": %s", p.stateFilePath, err))
	}

	// Check if main branch defined
	// Create definition if not exists
	found := false
	for _, branch := range p.stateFile.Branches {
		if branch.Branch.IsDefault {
			found = true
			break
		}
	}
	if !found {
		p.stateFile.Branches = append(p.stateFile.Branches, &BranchStateConfigName{
			Branch: &Branch{Name: "Main", IsDefault: true},
		})
	}
}

func (p *TestProject) createBranch(fixture *Branch, defaultBranch *model.Branch) *model.Branch {
	branch := fixture.ToModel(defaultBranch)
	p.api.
		CreateBranchRequest(branch).
		Send()
	p.log(`crated branch "%s", id: "%d"`, branch.Name, branch.Id)
	return branch
}

func (p *TestProject) getConfigFixture(name string) *model.Config {
	// Load
	path := filepath.Join(p.testDir, "configs", fmt.Sprintf("%s.json", name))
	content := utils.GetFileContent(path)
	fixture := &Config{}
	err := json.Unmarshal([]byte(content), fixture)
	if err != nil {
		panic(fmt.Errorf("cannot decode JSON file \"%s\": %s", path, err))
	}
	return fixture.ToModel()
}
