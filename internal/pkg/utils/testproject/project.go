package testproject

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"

	"github.com/keboola/go-client/pkg/encryptionapi"

	"github.com/keboola/go-client/pkg/schedulerapi"
	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type Project struct {
	*testproject.Project
	t                   *testing.T
	ctx                 context.Context
	storageApiClient    client.Client
	encryptionApiClient client.Client
	schedulerApiClient  client.Client
	storageApiToken     *storageapi.Token
	defaultBranch       *storageapi.Branch
	envs                *env.Map
	mapsLock            *sync.Mutex
	branchesById        map[storageapi.BranchID]*storageapi.Branch
	branchesByName      map[string]*storageapi.Branch
}

func GetTestProject(t *testing.T, envs *env.Map) *Project {
	ctx, cancelFn := context.WithCancel(context.Background())
	t.Cleanup(func() {
		// Cancel background jobs
		cancelFn()
	})

	p := &Project{Project: testproject.GetTestProject(t), t: t, ctx: ctx, mapsLock: &sync.Mutex{}}

	// Init storage API
	p.storageApiClient = storageapi.ClientWithHostAndToken(client.NewTestClient(), p.StorageAPIHost(), p.StorageAPIToken())

	// Load services
	index, err := storageapi.IndexRequest().Send(p.ctx, p.storageApiClient)
	if err != nil {
		assert.FailNow(p.t, "cannot get services: ", err)
	}
	services := index.Services.ToMap()

	// Get encryption service host
	encryptionHost, found := services.URLByID("encryption")
	if !found {
		assert.FailNow(p.t, "encryption service not found")
	}

	// Init Encryption API
	p.encryptionApiClient = encryptionapi.ClientWithHost(client.NewTestClient(), encryptionHost.String())

	// Get scheduler service host
	schedulerHost, found := services.URLByID("scheduler")
	if !found {
		assert.FailNow(p.t, "missing scheduler service")
	}

	// Init Scheduler API
	p.schedulerApiClient = schedulerapi.ClientWithHostAndToken(client.NewTestClient(), schedulerHost.String(), p.StorageAPIToken())

	// Check token/project ID
	initWg := &sync.WaitGroup{}
	initWg.Add(1)
	go func() {
		defer initWg.Done()
		if token, err := storageapi.VerifyTokenRequest(p.StorageAPIToken()).Send(p.ctx, p.storageApiClient); err != nil {
			assert.FailNow(p.t, "invalid token for project %d: %w", p.ID(), err)
		} else if p.ID() != token.ProjectID() {
			assert.FailNow(p.t, "test project id and token project id are different.")
		} else {
			p.storageApiToken = token
		}
	}()

	// Load default branch
	initWg.Add(1)
	go func() {
		defer initWg.Done()
		p.defaultBranch, err = storageapi.GetDefaultBranchRequest().Send(p.ctx, p.storageApiClient)
		if err != nil {
			assert.FailNow(p.t, "cannot get default branch: ", err)
		}
	}()

	// Set envs
	initWg.Wait()
	p.envs = envs.Clone()
	p.setEnv(`TEST_KBC_PROJECT_ID`, cast.ToString(p.ID()))
	p.setEnv(`TEST_KBC_PROJECT_NAME`, p.Name())
	p.setEnv(`TEST_KBC_STORAGE_API_HOST`, p.StorageAPIHost())
	p.setEnv(`TEST_KBC_STORAGE_API_TOKEN`, p.StorageAPIToken())
	p.logf(`Project locked`)
	return p
}

func (p *Project) Env() *env.Map {
	return p.envs
}

func (p *Project) DefaultBranch() *storageapi.Branch {
	return p.defaultBranch
}

func (p *Project) Name() string {
	return p.storageApiToken.ProjectName()
}

func (p *Project) StorageApiClient() client.Client {
	return p.storageApiClient
}

func (p *Project) StorageApiToken() *storageapi.Token {
	return p.storageApiToken
}

func (p *Project) EncryptionApiClient() client.Client {
	return p.encryptionApiClient
}

func (p *Project) SchedulerApiClient() client.Client {
	return p.schedulerApiClient
}

// Clean method resets default branch, deletes all project branches (except default), all configurations and all schedules.
func (p *Project) Clean() {
	p.logf("Clearing project ...")
	startTime := time.Now()

	grp, cancelFn := errgroup.WithContext(context.Background())
	defer cancelFn()

	// Clean by Storage API
	grp.Go(func() error {
		_, err := storageapi.CleanProjectRequest().Send(p.ctx, p.storageApiClient)
		return err
	})

	// Clean by Scheduler API
	grp.Go(func() error {
		_, err := schedulerapi.CleanAllSchedulesRequest().Send(p.ctx, p.schedulerApiClient)
		return err
	})

	if err := grp.Wait(); err != nil {
		p.t.Fatalf(`cannot clean project "%d": %s`, p.ID(), err)
	}
	p.logf("Test project cleared | %s", time.Since(startTime))
}

func (p *Project) SetState(stateFilePath string) {
	// Remove all objects
	p.Clean()
	p.branchesById = make(map[storageapi.BranchID]*storageapi.Branch)
	p.branchesByName = make(map[string]*storageapi.Branch)

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
	grp, cancelFn := errgroup.WithContext(context.Background())
	defer cancelFn()

	// Create branches
	for _, fixture := range branches {
		fixture := fixture
		grp.Go(func() error {
			var branch *storageapi.Branch
			if fixture.IsDefault {
				// Set default branch description
				if p.defaultBranch.Description != fixture.Description {
					if _, err := storageapi.UpdateBranchRequest(p.defaultBranch, []string{"description"}).Send(p.ctx, p.storageApiClient); err != nil {
						return fmt.Errorf("cannot set default branch description: %s", err)
					}
				}
				branch = p.defaultBranch
			} else {
				// Create a new branch
				if v, err := storageapi.CreateBranchRequest(fixture.ToApi()).Send(p.ctx, p.storageApiClient); err == nil {
					branch = v
				} else {
					return fmt.Errorf(`cannot create branch: %s`, err)
				}
			}

			// Add branch to aux maps
			p.addBranch(branch)

			// Set branch metadata
			_, err := storageapi.AppendBranchMetadataRequest(branch.BranchKey, fixture.Metadata).Send(p.ctx, p.storageApiClient)
			return err
		})
	}

	// Wait for all work
	if err := grp.Wait(); err != nil {
		assert.FailNow(p.t, err.Error())
	}
}

func (p *Project) createConfigsInDefaultBranch(configs []string) {
	ctx, cancelFn := context.WithCancel(p.ctx)
	defer cancelFn()

	tickets := storageapi.NewTicketProvider(ctx, p.storageApiClient)
	grp, ctx := errgroup.WithContext(ctx) // group for all parallel requests
	sendReady := make(chan struct{})      // block requests until IDs and ENVs will be ready

	// Prepare configs
	envPrefix := "TEST_BRANCH_ALL_CONFIG"
	p.prepareConfigs(ctx, grp, sendReady, tickets, envPrefix, configs, p.defaultBranch)

	// Generate new IDs
	if err := tickets.Resolve(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf(`cannot generate new IDs: %s`, err))
	}

	// Wait for requests
	close(sendReady) // unblock requests
	if err := grp.Wait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot create configs: %s", err))
	}
}

func (p *Project) createConfigs(branches []*fixtures.BranchState, additionalEnvs map[string]string) {
	ctx, cancelFn := context.WithCancel(p.ctx)
	defer cancelFn()

	tickets := storageapi.NewTicketProvider(ctx, p.storageApiClient)
	grp, ctx := errgroup.WithContext(ctx) // group for all parallel requests
	sendReady := make(chan struct{})      // block requests until IDs and ENVs will be ready

	// Prepare configs
	for _, branchFixture := range branches {
		envPrefix := fmt.Sprintf("TEST_BRANCH_%s_CONFIG", branchFixture.Name)
		p.prepareConfigs(ctx, grp, sendReady, tickets, envPrefix, branchFixture.Configs, p.branchesByName[branchFixture.Name])
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

	// Wait for requests
	close(sendReady) // unblock requests
	if err := grp.Wait(); err != nil {
		assert.FailNow(p.t, fmt.Sprintf("cannot create configs: %s", err))
	}
}

func (p *Project) prepareConfigs(ctx context.Context, grp *errgroup.Group, sendReady <-chan struct{}, tickets *storageapi.TicketProvider, envPrefix string, names []string, branch *storageapi.Branch) {
	for _, name := range names {
		configFixture := fixtures.LoadConfig(p.t, name)
		configWithRows := configFixture.ToApi()

		// Generate ID for config
		p.logf("creating IDs for config \"%s/%s/%s\"", branch.Name, configFixture.ComponentID, configFixture.Name)
		tickets.Request(func(ticket *storageapi.Ticket) {
			configWithRows.BranchID = branch.ID
			configWithRows.ID = storageapi.ConfigID(ticket.ID)
			p.setEnv(fmt.Sprintf("%s_%s_ID", envPrefix, configFixture.Name), configWithRows.ID.String())
		})

		// For each row
		for rowIndex, rowFixture := range configFixture.Rows {
			row := rowFixture.ToApi()

			// Generate name, if needed
			if len(row.Name) == 0 {
				row.Name = cast.ToString(rowIndex + 1)
			}

			// Generate ID for row
			tickets.Request(func(ticket *storageapi.Ticket) {
				row.ID = storageapi.RowID(ticket.ID)
				p.setEnv(fmt.Sprintf("%s_%s_ROW_%s_ID", envPrefix, configFixture.Name, row.Name), row.ID.String())
			})
		}

		// Create configs and rows
		grp.Go(func() error {
			// Wait for all IDs and ENVs
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-sendReady:
				// continue!
			}

			// Is context done?
			if err := ctx.Err(); err != nil {
				return err
			}

			// Replace ENVs in config and rows content
			json.MustDecodeString(testhelper.ReplaceEnvsString(json.MustEncodeString(configWithRows.Content, false), p.envs), &configWithRows.Content)
			for _, row := range configWithRows.Rows {
				json.MustDecodeString(testhelper.ReplaceEnvsString(json.MustEncodeString(row.Content, false), p.envs), &row.Content)
			}

			// Send request
			_, err := storageapi.
				CreateConfigRequest(configWithRows).
				WithOnSuccess(func(ctx context.Context, sender client.Sender, _ *storageapi.ConfigWithRows) error {
					if len(configFixture.Metadata) > 0 {
						_, err := storageapi.AppendConfigMetadataRequest(configWithRows.ConfigKey, configFixture.Metadata).Send(ctx, sender)
						return err
					}
					return nil
				}).
				Send(ctx, p.storageApiClient)
			return err
		})
	}
}

func (p *Project) addBranch(branch *storageapi.Branch) {
	p.logf(`branch "%s" is ready, id: "%d"`, branch.Name, branch.ID)
	p.setEnv(fmt.Sprintf("TEST_BRANCH_%s_ID", branch.Name), branch.ID.String())
	p.mapsLock.Lock()
	defer p.mapsLock.Unlock()
	p.branchesById[branch.ID] = branch
	p.branchesByName[branch.Name] = branch
}

// setEnv set ENV variable, all ENVs are logged at the end of SetState method.
func (p *Project) setEnv(key string, value string) {
	// Normalize key
	key = regexp.MustCompile(`[^a-zA-Z0-9]+`).ReplaceAllString(key, "_")
	key = strings.ToUpper(key)
	key = strings.Trim(key, "_")

	// Set
	p.envs.Set(key, value)
}

func (p *Project) logEnvs() {
	for _, item := range p.envs.ToSlice() {
		p.logf(fmt.Sprintf(`ENV "%s"`, item))
	}
}

func (p *Project) logf(format string, a ...interface{}) {
	if testhelper.TestIsVerbose() {
		a = append([]interface{}{p.ID(), p.t.Name()}, a...)
		p.t.Logf("TestProject[%d][%s]: "+format, a...)
	}
}
