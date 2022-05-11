// nolint forbidigo
package testproject

import (
	"context"
	"errors"
	"fmt"
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

	"github.com/keboola/keboola-as-code/internal/pkg/api/client/encryptionapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testapi"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type Project struct {
	t              *testing.T
	host           string // Storage API host
	token          string // Storage API token
	id             int    // project ID
	lock           *fslock.Lock
	locked         bool
	mutex          *sync.Mutex
	storageApi     *storageapi.Api
	encryptionApi  *encryptionapi.Api
	schedulerApi   *schedulerapi.Api
	defaultBranch  *model.Branch
	branchesById   map[model.BranchId]*model.Branch
	branchesByName map[string]*model.Branch
	envLock        *sync.Mutex
	envs           *env.Map
	newEnvs        []string
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

	p := &Project{
		host:    host,
		id:      id,
		token:   token,
		lock:    fslock.New(lockPath),
		mutex:   &sync.Mutex{},
		envLock: &sync.Mutex{},
	}

	// Init API
	p.storageApi, _ = testapi.NewStorageApiWithToken(p.host, p.token, testhelper.TestIsVerbose())

	// Load services
	services, err := p.storageApi.ServicesUrlById()
	if err != nil {
		assert.FailNow(p.t, "cannot get services: ", err)
	}

	// Get scheduler service host
	schedulerHost, found := services["scheduler"]
	if !found {
		assert.FailNow(p.t, "missing scheduler service")
	}

	// Init encrypt API

	// Init Scheduler API
	logger := log.NewDebugLogger()
	if testhelper.TestIsVerbose() {
		logger.ConnectTo(os.Stdout)
	}
	p.schedulerApi = schedulerapi.New(
		context.Background(),
		logger,
		string(schedulerHost),
		p.storageApi.Token().Token,
		false,
	)

	// Check project ID
	if p.id != p.storageApi.ProjectId() {
		assert.FailNow(p.t, "test project id and token project id are different.")
	}

	// Load default branch
	p.defaultBranch, err = p.storageApi.GetDefaultBranch()
	if err != nil {
		assert.FailNow(p.t, "cannot get default branch: ", err)
	}
	branchMetadataResponse := p.storageApi.ListBranchMetadataRequest(p.defaultBranch.Id).Send().Response
	branchMetadataMap := make(map[string]string)
	if branchMetadataResponse.HasResult() {
		metadata := *branchMetadataResponse.Result().(*[]storageapi.Metadata)
		for _, m := range metadata {
			branchMetadataMap[m.Key] = m.Value
		}
	}
	p.defaultBranch.Metadata = branchMetadataMap

	return p
}

func (p *Project) Id() int {
	p.assertLocked()
	return p.id
}

func (p *Project) DefaultBranch() *model.Branch {
	p.assertLocked()
	return p.defaultBranch
}

func (p *Project) Name() string {
	p.assertLocked()
	return p.storageApi.ProjectName()
}

func (p *Project) StorageApiHost() string {
	p.assertLocked()
	return p.host
}

func (p *Project) StorageApiToken() string {
	p.assertLocked()
	return p.storageApi.Token().Token
}

func (p *Project) StorageApi() *storageapi.Api {
	p.assertLocked()
	return p.storageApi
}

func (p *Project) EncryptionApi() *encryptionapi.Api {
	p.assertLocked()
	return p.encryptionApi
}

func (p *Project) SchedulerApi() *schedulerapi.Api {
	p.assertLocked()
	return p.schedulerApi
}

// Clear deletes all project branches (except default) and all configurations.
func (p *Project) Clear() {
	p.assertLocked()
	p.logf("Clearing project ...")
	startTime := time.Now()

	// Clear branches maps
	p.branchesById = make(map[model.BranchId]*model.Branch)
	p.branchesByName = make(map[string]*model.Branch)

	// Delete all configs in default branch, it cannot be deleted
	pool := p.storageApi.NewPool()
	pool.Request(p.storageApi.DeleteConfigsInBranchRequest(p.defaultBranch.BranchKey)).Send()
	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot delete branches: %s", err))
	}

	// Delete metadata of default branch
	pool = p.storageApi.NewPool()
	pool.Request(p.storageApi.ListBranchMetadataRequest(p.defaultBranch.Id)).
		OnSuccess(func(response *client.Response) {
			branchMetadataResponse := *response.Result().(*[]storageapi.Metadata)
			for _, m := range branchMetadataResponse {
				pool.Request(p.storageApi.DeleteBranchMetadataRequest(p.defaultBranch.Id, m.Id)).Send()
			}
		}).
		Send()
	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot delete metadata: %s", err))
	}

	// Load branches
	branches, err := p.storageApi.ListBranches()
	if err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot load branches: %s", err))
	}

	// Delete all dev-branches sequentially, parallel requests don't work with this endpoint
	for _, branch := range branches {
		if !branch.IsDefault {
			if _, err := p.storageApi.DeleteBranch(branch.BranchKey); err != nil {
				assert.FailNow(p.t, fmt.Sprintf("cannot delete branch: %s", err))
			}
		}
	}

	p.clearSchedules()

	p.logf("Test project cleared | %s", time.Since(startTime))
}

// Clear deletes all schedules.
func (p *Project) clearSchedules() {
	// Load schedules
	schedules, err := p.schedulerApi.ListSchedules()
	if err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot load schedules: %s", err))
	}

	// Delete all schedules
	pool := p.schedulerApi.NewPool()
	for _, schedule := range schedules {
		pool.Request(p.schedulerApi.DeleteScheduleRequest(schedule.Id)).Send()
	}
	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot delete schedules: %s", err))
	}
}

func (p *Project) SetState(stateFilePath string) {
	p.assertLocked()

	// Remove all objects
	p.Clear()

	// Log ENVs at the end
	defer p.logEnvs()

	// Load desired state from file
	// nolint: dogsled
	_, testFile, _, _ := runtime.Caller(0)
	testDir := filesystem.Dir(testFile)
	// nolint: forbidigo
	if !filepath.IsAbs(stateFilePath) {
		stateFilePath = filesystem.Join(testDir, "..", "..", "fixtures", "remote", stateFilePath)
	}

	// Load state file
	stateFile, err := fixtures.LoadStateFile(stateFilePath)
	if err != nil {
		assert.FailNow(p.t, err.Error())
	}

	// Set new state
	startTime := time.Now()
	p.logf("Setting project state ...")

	// Create configs in default branch, they will be auto-copied to branches created later
	p.createConfigsInDefaultBranch(stateFile.AllBranchesConfigs)

	// Create branches
	p.createBranches(stateFile.Branches)

	// Create configs in branches
	p.createConfigs(stateFile.Branches, stateFile.Envs)

	p.logf("Project state set | %s", time.Since(startTime))
}

func (p *Project) createBranches(branches []*fixtures.BranchState) {
	// Create branches sequentially, parallel requests don't work with this endpoint
	for _, fixture := range branches {
		branch := fixture.Branch.ToModel(p.defaultBranch)
		if len(fixture.Metadata) > 0 {
			branch.Metadata = fixture.Metadata
			p.StorageApi().AppendBranchMetadataRequest(branch).Send()
		}
		if branch.IsDefault {
			p.defaultBranch.Description = fixture.Branch.Description
			if _, err := p.storageApi.UpdateBranch(p.defaultBranch, model.NewChangedFields("description")); err != nil {
				assert.FailNow(p.t, fmt.Sprintf("cannot set default branch description: %s", err))
			}
			p.setEnv(fmt.Sprintf("TEST_BRANCH_%s_ID", branch.Name), branch.Id.String())
		} else {
			err := p.storageApi.
				CreateBranchRequest(branch).
				OnSuccess(func(response *client.Response) {
					p.logf(`crated branch "%s", id: "%d"`, branch.Name, branch.Id)
					p.setEnv(fmt.Sprintf("TEST_BRANCH_%s_ID", branch.Name), branch.Id.String())
					p.branchesById[branch.Id] = branch
					p.branchesByName[branch.Name] = branch
				}).
				Send().
				Err()
			if err != nil {
				assert.FailNow(p.t, fmt.Sprintf(`cannot create branch: %s`, err))
			}
		}
	}
}

func (p *Project) createConfigsInDefaultBranch(names []string) {
	p.branchesById[p.defaultBranch.Id] = p.defaultBranch
	p.branchesByName[p.defaultBranch.Name] = p.defaultBranch

	// Prepare configs
	tickets := p.storageApi.NewTicketProvider()
	configs := p.prepareConfigs(names, p.defaultBranch, tickets, "TEST_BRANCH_ALL_CONFIG")

	// Generate new IDs
	if err := tickets.Resolve(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf(`cannot generate new IDs: %s`, err))
	}

	// Create requests
	pool := p.storageApi.NewPool()
	p.createConfigsRequests(configs, pool)

	// Send requests
	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot create configs in default branch: %s", err))
	}
}

func (p *Project) createConfigs(branches []*fixtures.BranchState, additionalEnvs map[string]string) {
	// Prepare configs
	tickets := p.storageApi.NewTicketProvider()
	var configs []*model.ConfigWithRows
	for _, branch := range branches {
		modelBranch := p.branchesByName[branch.Branch.Name]
		envPrefix := fmt.Sprintf("TEST_BRANCH_%s_CONFIG", modelBranch.Name)
		configs = append(configs, p.prepareConfigs(branch.Configs, modelBranch, tickets, envPrefix)...)
	}

	// Generate new IDs
	if err := tickets.Resolve(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf(`cannot generate new IDs: %s`, err))
	}

	// Add additional ENVs
	if additionalEnvs != nil {
		for k, v := range additionalEnvs {
			p.setEnv(k, testhelper.ReplaceEnvsString(v, p.envs))
		}
	}

	// Create requests
	pool := p.storageApi.NewPool()
	p.createConfigsRequests(configs, pool)

	// Send requests
	if err := pool.StartAndWait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot create configs: %s", err))
	}
}

func (p *Project) createConfigsRequests(configs []*model.ConfigWithRows, pool *client.Pool) {
	for _, config := range configs {
		// Replace ENVs in config and rows content
		json.MustDecodeString(testhelper.ReplaceEnvsString(json.MustEncodeString(config.Content, false), p.envs), &config.Content)
		for _, row := range config.Rows {
			json.MustDecodeString(testhelper.ReplaceEnvsString(json.MustEncodeString(row.Content, false), p.envs), &row.Content)
		}

		// Create config request
		if request, err := p.storageApi.CreateConfigRequest(config); err == nil {
			branch := p.branchesById[config.BranchId]
			p.logf("creating config \"%s/%s/%s\"", branch.Name, config.ComponentId, config.Name)
			request.OnSuccess(func(response *client.Response) {
				if len(config.Config.Metadata) > 0 {
					request := p.StorageApi().AppendConfigMetadataRequest(config.Config)
					pool.Request(request).Send()
				}
			})
			pool.Request(request).Send()
		} else {
			assert.FailNow(p.t, fmt.Sprintf("cannot create create config request: %s", err))
		}
	}
}

func (p *Project) prepareConfigs(names []string, branch *model.Branch, tickets *storageapi.TicketProvider, envPrefix string) []*model.ConfigWithRows {
	var configs []*model.ConfigWithRows
	for _, name := range names {
		config := fixtures.LoadConfig(p.t, name)
		configs = append(configs, config)
		config.BranchId = branch.Id

		// Get IDs for config and its rows
		// In tests must be rows IDs order always equal
		p.logf("creating IDs for config \"%s/%s/%s\"", branch.Name, config.ComponentId, config.Name)
		tickets.Request(func(ticket *model.Ticket) {
			config.Id = model.ConfigId(ticket.Id)
			p.setEnv(fmt.Sprintf("%s_%s_ID", envPrefix, config.Name), config.Id.String())
		})
		for rowIndex, r := range config.Rows {
			row := r
			rowName := row.Name
			if len(rowName) == 0 {
				rowName = cast.ToString(rowIndex + 1)
			}
			tickets.Request(func(ticket *model.Ticket) {
				row.Id = model.RowId(ticket.Id)
				p.setEnv(fmt.Sprintf("%s_%s_ROW_%s_ID", envPrefix, config.Name, rowName), row.Id.String())
			})
		}
	}

	return configs
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
		p.logf(fmt.Sprintf(`ENV "%s"`, item))
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
	p.setEnv(`TEST_KBC_STORAGE_API_TOKEN`, p.StorageApiToken())
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
