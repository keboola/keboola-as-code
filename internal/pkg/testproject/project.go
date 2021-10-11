// nolint forbidigo
package testproject

import (
	"errors"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/juju/fslock"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/testhelper"
)

type Project struct {
	t             *testing.T
	host          string // Storage API host
	token         string // Storage API token
	id            int    // project ID
	lock          *fslock.Lock
	locked        bool
	mutex         *sync.Mutex
	api           *remote.StorageApi
	defaultBranch *model.Branch
	envLock       *sync.Mutex
	envs          *env.Map
	newEnvs       []string
}

// newProject - create test project handler and lock it.
func newProject(host string, id int, token string) *Project {
	// Create locks dir if not exists
	locksDir := filepath.Join(os.TempDir(), `.keboola-as-code-locks`)
	if err := os.MkdirAll(locksDir, 0o700); err != nil {
		panic(fmt.Errorf(`cannot lock test project: %s`, err))
	}

	// lock file name
	lockFile := host + `-` + cast.ToString(id) + `.lock`
	lockPath := filepath.Join(locksDir, lockFile)

	p :=  &Project{
		host:    host,
		id:      id,
		token:   token,
		lock:    fslock.New(lockPath),
		mutex:   &sync.Mutex{},
		envLock: &sync.Mutex{},
	}

	// Init API
	p.api, _ = testapi.TestStorageApiWithToken(p.host, p.token, testhelper.TestIsVerbose())

	// Check project ID
	if p.id != p.api.ProjectId() {
		assert.FailNow(p.t, "test project id and token project id are different.")
	}

	// Load default branch
	var err error
	p.defaultBranch, err = p.api.GetDefaultBranch()
	if err != nil {
		assert.FailNow(p.t, "cannot get default branch: ", err)
	}

	return p
}

func (p *Project) Id() int {
	p.assertLocked()
	return p.id
}

func (p *Project) StorageApiHost() string {
	p.assertLocked()
	return p.host
}

func (p *Project) Name() string {
	p.assertLocked()
	return p.api.ProjectName()
}

func (p *Project) Token() string {
	p.assertLocked()
	return p.api.Token().Token
}

func (p *Project) Api() *remote.StorageApi {
	p.assertLocked()
	return p.api
}

// Clear deletes all project branches (except default) and all configurations.
func (p *Project) Clear() {
	p.assertLocked()
	p.logf("Clearing project ...")
	startTime := time.Now()

	// Delete all configs in default branch, it cannot be deleted
	pool := p.api.NewPool()
	pool.Request(p.api.DeleteConfigsInBranchRequest(p.defaultBranch.Id)).Send()
	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot delete branches: %s", err))
	}

	// Load branches
	branches, err := p.api.ListBranches()
	if err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot load branches: %s", err))
	}

	// Delete all dev-branches sequentially, parallel requests don't work with this endpoint
	for _, branch := range branches {
		if !branch.IsDefault {
			p.api.DeleteBranchRequest(branch.Id).Send()
		}
	}

	p.logf("Test project cleared | %s", time.Since(startTime))
}

func (p *Project) SetState(stateFilePath string) {
	p.assertLocked()

	// Remove all objects
	p.Clear()

	// Load desired state from file
	// nolint: dogsled
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	// nolint: forbidigo
	if !filepath.IsAbs(stateFilePath) {
		stateFilePath = filesystem.Join(testDir, "..", "fixtures", "remote", stateFilePath)
	}

	// Load state file
	stateFile, err := fixtures.LoadStateFile(stateFilePath)
	if err != nil {
		assert.FailNow(p.t, err.Error())
	}

	// Set new state
	startTime := time.Now()
	p.logf("Setting project state ...")

	// Create configs in default branch, they will be auto-copied to dev-branches
	pool := p.api.NewPool()
	p.createConfigs(pool, stateFile.AllBranchesConfigs, p.defaultBranch, "TEST_BRANCH_ALL_CONFIG")
	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot create configs in default branch: %s", err))
	}

	// Create branches sequentially, parallel requests don't work with this endpoint
	branchesByName := make(map[string]*model.Branch)
	for _, fixture := range stateFile.Branches {
		branch := fixture.Branch.ToModel(p.defaultBranch)
		branchesByName[branch.Name] = branch
		if branch.IsDefault {
			p.defaultBranch.Description = fixture.Branch.Description
			if _, err := p.api.UpdateBranch(p.defaultBranch, []string{"description"}); err != nil {
				assert.FailNow(p.t, fmt.Sprintf("cannot set default branch description: %s", err))
			}
			p.setEnv(fmt.Sprintf("TEST_BRANCH_%s_ID", branch.Name), cast.ToString(branch.Id))
		} else {
			p.api.
				CreateBranchRequest(branch).
				OnSuccess(func(response *client.Response) {
					p.logf(`crated branch "%s", id: "%d"`, branch.Name, branch.Id)
					p.setEnv(fmt.Sprintf("TEST_BRANCH_%s_ID", branch.Name), cast.ToString(branch.Id))
				}).
				Send()
		}
	}

	// Create configs in dev-branches
	pool = p.api.NewPool()
	for _, branch := range stateFile.Branches {
		modelBranch := branchesByName[branch.Branch.Name]
		envPrefix := fmt.Sprintf("TEST_BRANCH_%s_CONFIG", modelBranch.Name)
		p.createConfigs(pool, branch.Configs, modelBranch, envPrefix)
	}
	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot create configs: %s", err))
	}

	p.logEnvs()
	p.logf("Project state set | %s", time.Since(startTime))
}

// createConfigs loads configs from files and creates them in the test project.
func (p *Project) createConfigs(pool *client.Pool, names []string, branch *model.Branch, envPrefix string) {
	for _, name := range names {
		config := fixtures.LoadConfig(p.t, name)
		config.BranchId = branch.Id

		// Get IDs for config and its rows
		// In tests must be rows IDs order always equal
		p.logf("creating IDs for config \"%s/%s/%s\"", branch.Name, config.ComponentId, config.Name)
		tickets := p.api.NewTicketProvider()
		tickets.Request(func(ticket *model.Ticket) {
			config.Id = ticket.Id
			p.setEnv(fmt.Sprintf("%s_%s_ID", envPrefix, config.Name), config.Id)
		})
		for rowIndex, r := range config.Rows {
			row := r
			rowName := row.Name
			if len(rowName) == 0 {
				rowName = cast.ToString(rowIndex + 1)
			}
			tickets.Request(func(ticket *model.Ticket) {
				row.Id = ticket.Id
				p.setEnv(fmt.Sprintf("%s_%s_ROW_%s_ID", envPrefix, config.Name, rowName), row.Id)
			})
		}
		if err := tickets.Resolve(); err != nil {
			assert.FailNow(p.t, fmt.Sprintf(`cannot generate new IDs: %s`, err))
		}

		// Create config and rows, set ENVs
		if request, err := p.api.CreateConfigRequest(config); err == nil {
			p.logf("creating config \"%s/%s/%s\"", branch.Name, config.ComponentId, config.Name)
			pool.Request(request).Send()
		} else {
			assert.FailNow(p.t, fmt.Sprintf("cannot create create config request: %s", err))
		}
	}
}

// setEnv set ENV variable, all ENVs are logged at the end of SetState method.
func (p *Project) setEnv(key string, value string) {
	// Normalize key
	key = regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(key, "_")
	key = strings.ToUpper(key)
	key = strings.Trim(key, "_")

	// Set
	p.envs.Set(key, value)

	// Log
	p.envLock.Lock()
	defer p.envLock.Unlock()
	p.newEnvs = append(p.newEnvs, fmt.Sprintf("%s=%s", key, value))
}

func (p *Project) logEnvs() {
	for _, item := range p.newEnvs {
		p.logf(fmt.Sprintf(`Set ENV "%s"`, item))
	}
}

func (p *Project) logf(format string, a ...interface{}) {
	if testhelper.TestIsVerbose() {
		a = append([]interface{}{p.id, p.t.Name()}, a...)
		p.t.Logf("TestProject[%d][%s]: "+format, a...)
	}
}

func (p *Project) assertLocked() {
	if !p.locked {
		panic(fmt.Errorf(`test project "%d" is not locked`, p.id))
	}
}

func (p *Project) tryLock(t *testing.T, envs *env.Map) bool {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.locked {
		return false
	}

	if err := p.lock.TryLock(); err != nil {
		if !errors.Is(err, fslock.ErrLocked) {
			// Unexpected error
			panic(err)
		}

		// Busy
		return false
	}

	// Locked!
	p.t = t
	p.locked = true

	// Unlock, when test is done
	p.t.Cleanup(func() {
		p.unlock()
	})

	// Set ENVs, the environment resets when unlock is called
	p.envs = envs
	p.newEnvs = make([]string, 0)
	p.setEnv(`TEST_KBC_PROJECT_ID`, cast.ToString(p.Id()))
	p.setEnv(`TEST_KBC_PROJECT_NAME`, p.Name())
	p.setEnv(`TEST_KBC_STORAGE_API_HOST`, p.StorageApiHost())
	p.setEnv(`TEST_KBC_STORAGE_API_TOKEN`, p.Token())
	p.logf(`Project locked`)

	return true
}

// unlock project if it is no more needed in test.
func (p *Project) unlock() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.newEnvs = make([]string, 0)
	p.envs = nil
	p.locked = false
	p.logf(`Project unlocked`)
	p.t = nil

	if err := p.lock.Unlock(); err != nil {
		panic(fmt.Errorf(`cannot unlock test project: %w`, err))
	}
}
