package testproject

import (
	"context"
	"fmt"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/encryptionapi"
	"github.com/keboola/go-client/pkg/schedulerapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type Project struct {
	*testproject.Project
	initStartedAt       time.Time
	ctx                 context.Context
	storageAPIToken     *storageapi.Token
	storageApiClient    client.Client
	encryptionAPIClient client.Client
	schedulerAPIClient  client.Client
	defaultBranch       *storageapi.Branch
	envs                *env.Map
	mapsLock            *sync.Mutex
	branchesById        map[storageapi.BranchID]*storageapi.Branch
	branchesByName      map[string]*storageapi.Branch
}

type UnlockFn func()

func GetTestProjectForTest(t *testing.T, envs *env.Map) *Project {
	t.Helper()

	p, unlockFn, err := GetTestProject(envs)
	assert.NoError(t, err)

	t.Cleanup(func() {
		// Unlock and cancel background jobs
		unlockFn()
	})

	return p
}

func GetTestProject(envs *env.Map) (*Project, UnlockFn, error) {
	project, unlockFn, err := testproject.GetTestProject()
	if err != nil {
		return nil, nil, err
	}

	ctx, cancelFn := context.WithCancel(context.Background())
	p := &Project{Project: project, initStartedAt: time.Now(), ctx: ctx, mapsLock: &sync.Mutex{}}
	p.logf("□ Initializing project...")

	cleanupFn := func() {
		cancelFn()
		unlockFn()
	}

	// Init storage API
	p.storageApiClient = storageapi.ClientWithHostAndToken(client.NewTestClient(), p.StorageAPIHost(), p.Project.StorageAPIToken())

	// Load services
	index, err := storageapi.IndexRequest().Send(p.ctx, p.storageApiClient)
	if err != nil {
		cleanupFn()
		return nil, nil, fmt.Errorf("cannot get services: %w", err)
	}
	services := index.Services.ToMap()

	// Get encryption service host
	encryptionHost, found := services.URLByID("encryption")
	if !found {
		cleanupFn()
		return nil, nil, fmt.Errorf("encryption service not found")
	}

	// Init Encryption API
	p.encryptionAPIClient = encryptionapi.ClientWithHost(client.NewTestClient(), encryptionHost.String())

	// Get scheduler service host
	schedulerHost, found := services.URLByID("scheduler")
	if !found {
		cleanupFn()
		return nil, nil, fmt.Errorf("missing scheduler service")
	}

	// Init Scheduler API
	p.schedulerAPIClient = schedulerapi.ClientWithHostAndToken(client.NewTestClient(), schedulerHost.String(), p.Project.StorageAPIToken())

	// Check token/project ID
	errors := utils.NewMultiError()
	initWg := &sync.WaitGroup{}
	initWg.Add(1)
	go func() {
		defer initWg.Done()
		if token, err := storageapi.VerifyTokenRequest(p.Project.StorageAPIToken()).Send(p.ctx, p.storageApiClient); err != nil {
			errors.Append(fmt.Errorf("invalid token for project %d: %s", p.ID(), err))
		} else if p.ID() != token.ProjectID() {
			errors.Append(fmt.Errorf("test project id and token project id are different"))
		} else {
			p.storageAPIToken = token
		}
	}()
	initWg.Wait()
	if len(errors.Errors) > 0 {
		cleanupFn()
		return nil, nil, errors
	}

	// Set envs
	p.envs = envs.Clone()
	p.setEnv(`TEST_KBC_PROJECT_ID`, cast.ToString(p.ID()))
	p.setEnv(`TEST_KBC_STORAGE_API_HOST`, p.Project.StorageAPIHost())
	p.setEnv(`TEST_KBC_STORAGE_API_TOKEN`, p.Project.StorageAPIToken())
	p.logf(`■ ️Initialization done.`)
	return p, cleanupFn, nil
}

func (p *Project) Env() *env.Map {
	return p.envs
}

func (p *Project) DefaultBranch() (*storageapi.Branch, error) {
	if p.defaultBranch == nil {
		if v, err := storageapi.GetDefaultBranchRequest().Send(p.ctx, p.storageApiClient); err != nil {
			p.defaultBranch = v
		} else {
			return nil, fmt.Errorf("cannot get default branch: %w", err)
		}
	}
	return p.defaultBranch, nil
}

func (p *Project) StorageAPIToken() *storageapi.Token {
	return p.storageAPIToken
}

func (p *Project) StorageAPIClient() client.Client {
	return p.storageApiClient
}

func (p *Project) EncryptionAPIClient() client.Client {
	return p.encryptionAPIClient
}

func (p *Project) SchedulerAPIClient() client.Client {
	return p.schedulerAPIClient
}

// Clean method resets default branch, deletes all project branches (except default), all configurations and all schedules.
func (p *Project) Clean() error {
	p.logf("□ Cleaning project...")

	ctx, cancelFn := context.WithCancel(context.Background())
	grp, ctx := errgroup.WithContext(ctx)
	defer cancelFn()

	// Clean by Storage API
	grp.Go(func() (err error) {
		p.defaultBranch, err = storageapi.
			CleanProjectRequest().
			WithOnSuccess(func(ctx context.Context, sender client.Sender, defaultBranch *storageapi.Branch) error {
				p.defaultBranch = defaultBranch
				return nil
			}).
			Send(ctx, p.storageApiClient)
		return err
	})

	// Clean by Scheduler API
	grp.Go(func() error {
		return schedulerapi.
			CleanAllSchedulesRequest().
			SendOrErr(ctx, p.schedulerAPIClient)
	})

	if err := grp.Wait(); err != nil {
		return fmt.Errorf(`cannot clean project "%d": %s`, p.ID(), err)
	}
	p.logf("■ Cleanup done.")
	return nil
}

func (p *Project) SetState(stateFilePath string) error {
	// Remove all objects
	err := p.Clean()
	if err != nil {
		return err
	}
	p.branchesById = make(map[storageapi.BranchID]*storageapi.Branch)
	p.branchesByName = make(map[string]*storageapi.Branch)

	// Set new state
	p.logf("□ Setting project state ...")

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
		return err
	}

	// Create configs in default branch, they will be auto-copied to branches created later
	err = p.createConfigsInDefaultBranch(stateFile.AllBranchesConfigs)
	if err != nil {
		return err
	}

	// Create branches
	err = p.createBranches(stateFile.Branches)
	if err != nil {
		return err
	}

	// Create configs in branches
	err = p.createConfigs(stateFile.Branches, stateFile.Envs)
	if err != nil {
		return err
	}

	p.logf("■ Project state set.")
	return nil
}

func (p *Project) createBranches(branches []*fixtures.BranchState) error {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	// Only one create branch request can run simultaneously.
	// Branch deletion is performed via Storage Job, which uses locks.
	// If we ran multiple requests, then only one job would run and the other jobs would wait.
	// The problem is that the lock is checked again after 30 seconds, so there is a long delay.
	createBranchSem := semaphore.NewWeighted(1)

	// Create branches
	grp := client.NewWaitGroup(ctx, p.storageApiClient)
	for _, fixture := range branches {
		fixture := fixture
		grp.Send(p.createBranchRequest(fixture, createBranchSem))
	}

	// Wait for all requests
	if err := grp.Wait(); err != nil {
		return err
	}
	return nil
}

func (p *Project) createBranchRequest(fixture *fixtures.BranchState, createBranchSem *semaphore.Weighted) client.APIRequest[*storageapi.Branch] {
	var request client.APIRequest[*storageapi.Branch]

	// Create branch
	if fixture.IsDefault {
		// Reset default branch description (default branch cannot be created/deleted)
		request = client.NewNoOperationAPIRequest(p.defaultBranch) // default branch already exists
		if p.defaultBranch.Description != fixture.Description {
			request = request.WithOnSuccess(func(ctx context.Context, sender client.Sender, branch *storageapi.Branch) error {
				branch.Description = fixture.Description
				return storageapi.
					UpdateBranchRequest(branch, []string{"description"}).
					WithBefore(func(ctx context.Context, sender client.Sender) error {
						p.logf("▶ Default branch description ...")
						return nil
					}).
					WithOnComplete(func(ctx context.Context, sender client.Sender, _ *storageapi.Branch, err error) error {
						if err == nil {
							p.logf("✔️ Default branch description.")
							return nil
						} else {
							return fmt.Errorf("cannot set default branch description: %w", err)
						}
					}).
					SendOrErr(ctx, sender)
			})
		}
	} else {
		// Create a new branch
		request = storageapi.
			CreateBranchRequest(fixture.ToApi()).
			WithBefore(func(ctx context.Context, _ client.Sender) error {
				p.logf("▶ Branch \"%s\"...", fixture.Name)
				return createBranchSem.Acquire(ctx, 1)
			}).
			WithOnComplete(func(ctx context.Context, sender client.Sender, branch *storageapi.Branch, err error) error {
				createBranchSem.Release(1)
				if err == nil {
					p.logf("✔️ Branch \"%s\"(%s).", fixture.Name, branch.ID)
					return nil
				} else {
					return fmt.Errorf(`cannot create branch: %w`, err)
				}
			})
	}

	// Branch is ready
	request = request.WithOnSuccess(func(ctx context.Context, sender client.Sender, branch *storageapi.Branch) error {
		p.addBranch(branch)
		return nil
	})

	// Set branch metadata
	request = request.WithOnSuccess(func(ctx context.Context, sender client.Sender, branch *storageapi.Branch) error {
		return storageapi.
			AppendBranchMetadataRequest(branch.BranchKey, fixture.Metadata).
			WithBefore(func(ctx context.Context, sender client.Sender) error {
				p.logf("▶ Branch metadata \"%s\"...", fixture.Name)
				return nil
			}).
			WithOnComplete(func(ctx context.Context, sender client.Sender, _ client.NoResult, err error) error {
				if err == nil {
					p.logf("✔️ Branch metadata \"%s\".", fixture.Name)
					return nil
				} else {
					return fmt.Errorf(`cannot set branch metadata: %w`, err)
				}
			}).
			SendOrErr(ctx, sender)
	})
	return request
}

func (p *Project) createConfigsInDefaultBranch(configs []string) error {
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
		return fmt.Errorf(`cannot generate new IDs: %s`, err)
	}

	// Wait for requests
	close(sendReady) // unblock requests
	if err := grp.Wait(); err != nil {
		return fmt.Errorf("cannot create configs: %s", err)
	}
	return nil
}

func (p *Project) createConfigs(branches []*fixtures.BranchState, additionalEnvs map[string]string) error {
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
		return fmt.Errorf(`cannot generate new IDs: %s`, err)
	}

	// Add additional ENVs
	for k, v := range additionalEnvs {
		p.setEnv(k, testhelper.ReplaceEnvsString(v, p.envs))
	}

	// Wait for requests
	close(sendReady) // unblock requests
	if err := grp.Wait(); err != nil {
		return fmt.Errorf("cannot create configs: %s", err)
	}
	return nil
}

func (p *Project) prepareConfigs(ctx context.Context, grp *errgroup.Group, sendReady <-chan struct{}, tickets *storageapi.TicketProvider, envPrefix string, names []string, branch *storageapi.Branch) {
	for _, name := range names {
		configFixture := fixtures.LoadConfig(name)
		configWithRows := configFixture.ToApi()
		configDesc := fmt.Sprintf("%s/%s/%s", branch.Name, configFixture.ComponentID, configFixture.Name)

		// Generate ID for config
		p.logf("▶ ID for config \"%s\"...", configDesc)
		tickets.Request(func(ticket *storageapi.Ticket) {
			configWithRows.BranchID = branch.ID
			configWithRows.ID = storageapi.ConfigID(ticket.ID)
			p.setEnv(fmt.Sprintf("%s_%s_ID", envPrefix, configFixture.Name), configWithRows.ID.String())
			p.logf("✔️ ID for config \"%s\".", configDesc)
		})

		// For each row
		for rowIndex, row := range configWithRows.Rows {
			rowIndex, row := rowIndex, row
			rowDesc := fmt.Sprintf("%s/%s", configDesc, row.Name)

			// Generate ID for row
			p.logf("▶ ID for config row \"%s\"...", rowDesc)
			tickets.Request(func(ticket *storageapi.Ticket) {
				row.ID = storageapi.RowID(ticket.ID)

				// Generate row name for ENV, if needed
				rowName := row.Name
				if len(rowName) == 0 {
					rowName = cast.ToString(rowIndex + 1)
				}
				p.setEnv(fmt.Sprintf("%s_%s_ROW_%s_ID", envPrefix, configFixture.Name, rowName), row.ID.String())
				p.logf("✔️ ID for config row \"%s\".", rowDesc)
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

			// Replace ENVs in config and rows content
			json.MustDecodeString(testhelper.ReplaceEnvsString(json.MustEncodeString(configWithRows.Content, false), p.envs), &configWithRows.Content)
			for _, row := range configWithRows.Rows {
				json.MustDecodeString(testhelper.ReplaceEnvsString(json.MustEncodeString(row.Content, false), p.envs), &row.Content)
			}

			// Send request
			_, err := storageapi.
				CreateConfigRequest(configWithRows).
				WithBefore(func(ctx context.Context, sender client.Sender) error {
					p.logf("▶ Config \"%s/%s/%s\"...", branch.Name, configFixture.ComponentID, configFixture.Name)
					return nil
				}).
				WithOnSuccess(func(ctx context.Context, sender client.Sender, _ *storageapi.ConfigWithRows) error {
					p.logf("✔️️ Config \"%s/%s/%s\"...", branch.Name, configFixture.ComponentID, configFixture.Name)
					if len(configFixture.Metadata) > 0 {
						_, err := storageapi.
							AppendConfigMetadataRequest(configWithRows.ConfigKey, configFixture.Metadata).
							WithBefore(func(ctx context.Context, sender client.Sender) error {
								p.logf("▶ Config metadata \"%s\"...", configDesc)
								return nil
							}).
							WithOnSuccess(func(_ context.Context, _ client.Sender, _ client.NoResult) error {
								p.logf("✔️️ Config metadata \"%s\".", configDesc)
								return nil
							}).
							Send(ctx, sender)
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
		seconds := float64(time.Since(p.initStartedAt).Milliseconds()) / 1000
		a = append([]interface{}{p.ID(), seconds}, a...)
		fmt.Printf("TestProject[%d][%05.2fs]: "+format, a...)
	}
}
