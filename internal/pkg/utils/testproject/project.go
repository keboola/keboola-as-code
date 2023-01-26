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

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type Project struct {
	*testproject.Project
	initStartedAt    time.Time
	ctx              context.Context
	storageAPIToken  *keboola.Token
	keboolaAPIClient *keboola.API
	defaultBranch    *keboola.Branch
	envs             *env.Map
	mapsLock         *sync.Mutex
	stateFilePath    string
	branchesByID     map[keboola.BranchID]*keboola.Branch
	branchesByName   map[string]*keboola.Branch
	logFn            func(format string, a ...interface{})
}

type UnlockFn func()

func GetTestProjectForTest(t *testing.T) *Project {
	t.Helper()

	p, unlockFn, err := GetTestProject(env.Empty())
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		// Unlock and cancel background jobs
		unlockFn()
	})

	p.logFn = func(format string, a ...interface{}) {
		seconds := float64(time.Since(p.initStartedAt).Milliseconds()) / 1000
		a = append([]interface{}{p.ID(), t.Name(), seconds}, a...)
		t.Logf("TestProject[%d][%s][%05.2fs]: "+format, a...)
	}

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
	httpClient := client.NewTestClient()
	p.keboolaAPIClient = keboola.NewAPI(p.StorageAPIHost(), keboola.WithClient(&httpClient), keboola.WithToken(p.Project.StorageAPIToken()))

	// Check token/project ID
	errs := errors.NewMultiError()
	initWg := &sync.WaitGroup{}
	initWg.Add(1)
	go func() {
		defer initWg.Done()
		if token, err := p.keboolaAPIClient.VerifyTokenRequest(p.Project.StorageAPIToken()).Send(p.ctx); err != nil {
			errs.Append(errors.Errorf("invalid token for project %d: %w", p.ID(), err))
		} else if p.ID() != token.ProjectID() {
			errs.Append(errors.New("test project id and token project id are different"))
		} else {
			p.storageAPIToken = token
		}
	}()
	initWg.Wait()
	if errs.Len() > 0 {
		cleanupFn()
		return nil, nil, errs
	}

	// Set envs
	p.envs = envs.Clone()
	p.setEnv(`TEST_KBC_PROJECT_ID`, cast.ToString(p.ID()))
	p.setEnv(`TEST_KBC_PROJECT_STAGING_STORAGE`, p.Project.StagingStorage())
	p.setEnv(`TEST_KBC_STORAGE_API_HOST`, p.Project.StorageAPIHost())
	p.setEnv(`TEST_KBC_STORAGE_API_TOKEN`, p.Project.StorageAPIToken())
	p.logf(`■ ️Initialization done.`)

	// Remove all objects
	if err := p.Clean(); err != nil {
		cleanupFn()
		return nil, nil, err
	}

	return p, cleanupFn, nil
}

func (p *Project) Env() *env.Map {
	return p.envs
}

func (p *Project) DefaultBranch() (*keboola.Branch, error) {
	if p.defaultBranch == nil {
		if v, err := p.keboolaAPIClient.GetDefaultBranchRequest().Send(p.ctx); err == nil {
			p.defaultBranch = v
		} else {
			return nil, errors.Errorf("cannot get default branch: %w", err)
		}
	}
	return p.defaultBranch, nil
}

func (p *Project) StorageAPIToken() *keboola.Token {
	return p.storageAPIToken
}

func (p *Project) KeboolaAPIClient() *keboola.API {
	return p.keboolaAPIClient
}

// Clean method deletes all project branches (except default), all configurations, all schedules, and all sandboxes.
// It also sets the project's default branch.
func (p *Project) Clean() error {
	p.logf("□ Cleaning project...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Clean whole project - configs, buckets, schedules, sandbox instances, etc.
	if err := keboola.CleanProject(ctx, p.keboolaAPIClient); err != nil {
		return errors.Errorf(`cannot clean project "%d": %w`, p.ID(), err)
	}

	defaultBranch, err := p.keboolaAPIClient.GetDefaultBranchRequest().Send(ctx)
	if err != nil {
		return errors.Errorf(`cannot fetch default branch in project "%d": %w`, p.ID(), err)
	}

	p.stateFilePath = ""
	p.defaultBranch = defaultBranch
	p.branchesByID = make(map[keboola.BranchID]*keboola.Branch)
	p.branchesByName = make(map[string]*keboola.Branch)

	p.logf("■ Cleanup done.")
	return nil
}

func (p *Project) SetState(stateFilePath string) error {
	if p.stateFilePath != "" {
		return errors.New("SetState method can be called only once after the Clean method")
	}
	p.stateFilePath = stateFilePath

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

	// Create buckets and tables
	err = p.createBucketsTables(stateFile.Buckets)
	if err != nil {
		return err
	}

	// Create sandboxes in default branch
	err = p.createSandboxes(p.defaultBranch.ID, stateFile.Sandboxes)
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
	grp := client.NewWaitGroup(ctx)
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

func (p *Project) createBucketsTables(buckets []*fixtures.Bucket) error {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	// Create buckets and tables
	grp := client.NewWaitGroup(ctx)
	for _, b := range buckets {
		req := p.keboolaAPIClient.
			CreateBucketRequest(&keboola.Bucket{
				ID:          b.ID,
				Description: b.Description,
			}).
			WithBefore(func(ctx context.Context) error {
				p.logf("▶ Bucket \"%s.c-%s\"...", b.ID.Stage, b.ID.BucketName)
				return nil
			}).
			WithOnComplete(func(ctx context.Context, apiBucket *keboola.Bucket, err error) error {
				if err == nil {
					p.logf("✔️ Bucket \"%s\".", apiBucket.ID)

					for _, t := range b.Tables {
						p.logf("▶ Table \"%s\"...", t.Name)
						_, err = p.keboolaAPIClient.CreateTable(ctx, t.ID, t.Columns, keboola.WithPrimaryKey(t.PrimaryKey))
						if err != nil {
							return err
						}
						p.logf("✔️ Table \"%s\"(%s).", t.Name, t.ID)
					}

					return nil
				} else {
					return errors.Errorf(`cannot create bucket "%s": %w`, b.ID.String(), err)
				}
			})
		grp.Send(req)
	}
	if err := grp.Wait(); err != nil {
		return err
	}

	return nil
}

func (p *Project) createSandboxes(defaultBranchID keboola.BranchID, sandboxes []*fixtures.Sandbox) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()

	for _, fixture := range sandboxes {
		fixture := fixture

		wg.Add(1)
		go func() {
			defer wg.Done()

			opts := make([]keboola.CreateWorkspaceOption, 0)
			if keboola.WorkspaceSupportsSizes(fixture.Type) && len(fixture.Size) > 0 {
				opts = append(opts, keboola.WithSize(fixture.Size))
			}

			p.logf("▶ Sandbox \"%s\"...", fixture.Name)
			sandbox, err := p.keboolaAPIClient.CreateWorkspace(
				ctx,
				defaultBranchID,
				fixture.Name,
				fixture.Type,
				opts...,
			)
			if err != nil {
				errs.Append(errors.Errorf("could not create sandbox \"%s\": %w", fixture.Name, err))
				return
			}
			p.logf("✔️ Sandbox \"%s\"(%s).", sandbox.Config.Name, sandbox.Config.ID)
			p.setEnv(fmt.Sprintf("TEST_SANDBOX_%s_ID", fixture.Name), sandbox.Config.ID.String())
		}()
	}

	wg.Wait()
	if errs.Len() > 0 {
		return errs
	}

	return nil
}

func (p *Project) createBranchRequest(fixture *fixtures.BranchState, createBranchSem *semaphore.Weighted) client.APIRequest[*keboola.Branch] {
	var request client.APIRequest[*keboola.Branch]

	// Create branch
	if fixture.IsDefault {
		// Reset default branch description (default branch cannot be created/deleted)
		request = client.NewNoOperationAPIRequest(p.defaultBranch) // default branch already exists
		if p.defaultBranch.Description != fixture.Description {
			request = request.WithOnSuccess(func(ctx context.Context, branch *keboola.Branch) error {
				branch.Description = fixture.Description
				return p.keboolaAPIClient.
					UpdateBranchRequest(branch, []string{"description"}).
					WithBefore(func(ctx context.Context) error {
						p.logf("▶ Default branch description ...")
						return nil
					}).
					WithOnComplete(func(ctx context.Context, _ *keboola.Branch, err error) error {
						if err == nil {
							p.logf("✔️ Default branch description.")
							return nil
						} else {
							return errors.Errorf("cannot set default branch description: %w", err)
						}
					}).
					SendOrErr(ctx)
			})
		}
	} else {
		// Create a new branch
		request = p.keboolaAPIClient.
			CreateBranchRequest(fixture.ToAPI()).
			WithBefore(func(ctx context.Context) error {
				p.logf("▶ Branch \"%s\"...", fixture.Name)
				return createBranchSem.Acquire(ctx, 1)
			}).
			WithOnComplete(func(ctx context.Context, branch *keboola.Branch, err error) error {
				createBranchSem.Release(1)
				if err == nil {
					p.logf("✔️ Branch \"%s\"(%s).", fixture.Name, branch.ID)
					return nil
				} else {
					return errors.Errorf(`cannot create branch: %w`, err)
				}
			})
	}

	// Branch is ready
	request = request.WithOnSuccess(func(ctx context.Context, branch *keboola.Branch) error {
		p.addBranch(branch)
		return nil
	})

	// Set branch metadata
	request = request.WithOnSuccess(func(ctx context.Context, branch *keboola.Branch) error {
		return p.keboolaAPIClient.
			AppendBranchMetadataRequest(branch.BranchKey, fixture.Metadata).
			WithBefore(func(ctx context.Context) error {
				p.logf("▶ Branch metadata \"%s\"...", fixture.Name)
				return nil
			}).
			WithOnComplete(func(ctx context.Context, _ client.NoResult, err error) error {
				if err == nil {
					p.logf("✔️ Branch metadata \"%s\".", fixture.Name)
					return nil
				} else {
					return errors.Errorf(`cannot set branch metadata: %w`, err)
				}
			}).
			SendOrErr(ctx)
	})
	return request
}

func (p *Project) createConfigsInDefaultBranch(configs []string) error {
	ctx, cancelFn := context.WithCancel(p.ctx)
	defer cancelFn()

	tickets := keboola.NewTicketProvider(ctx, p.keboolaAPIClient)
	grp, ctx := errgroup.WithContext(ctx) // group for all parallel requests
	sendReady := make(chan struct{})      // block requests until IDs and ENVs will be ready

	// Prepare configs
	envPrefix := "TEST_BRANCH_ALL_CONFIG"
	p.prepareConfigs(ctx, grp, sendReady, tickets, envPrefix, configs, p.defaultBranch)

	// Generate new IDs
	if err := tickets.Resolve(); err != nil {
		return errors.Errorf(`cannot generate new IDs: %w`, err)
	}

	// Wait for requests
	close(sendReady) // unblock requests
	if err := grp.Wait(); err != nil {
		return errors.Errorf("cannot create configs: %w", err)
	}
	return nil
}

func (p *Project) createConfigs(branches []*fixtures.BranchState, additionalEnvs map[string]string) error {
	ctx, cancelFn := context.WithCancel(p.ctx)
	defer cancelFn()

	tickets := keboola.NewTicketProvider(ctx, p.keboolaAPIClient)
	grp, ctx := errgroup.WithContext(ctx) // group for all parallel requests
	sendReady := make(chan struct{})      // block requests until IDs and ENVs will be ready

	// Prepare configs
	for _, branchFixture := range branches {
		envPrefix := fmt.Sprintf("TEST_BRANCH_%s_CONFIG", branchFixture.Name)
		p.prepareConfigs(ctx, grp, sendReady, tickets, envPrefix, branchFixture.Configs, p.branchesByName[branchFixture.Name])
	}

	// Generate new IDs
	if err := tickets.Resolve(); err != nil {
		return errors.Errorf(`cannot generate new IDs: %w`, err)
	}

	// Add additional ENVs
	for k, v := range additionalEnvs {
		p.setEnv(k, testhelper.MustReplaceEnvsString(v, p.envs))
	}

	// Wait for requests
	close(sendReady) // unblock requests
	if err := grp.Wait(); err != nil {
		return errors.Errorf("cannot create configs: %w", err)
	}
	return nil
}

func (p *Project) prepareConfigs(ctx context.Context, grp *errgroup.Group, sendReady <-chan struct{}, tickets *keboola.TicketProvider, envPrefix string, names []string, branch *keboola.Branch) {
	for _, name := range names {
		configFixture := fixtures.LoadConfig(name)
		configWithRows := configFixture.ToAPI()
		configDesc := fmt.Sprintf("%s/%s/%s", branch.Name, configFixture.ComponentID, configFixture.Name)

		// Generate ID for config
		p.logf("▶ ID for config \"%s\"...", configDesc)
		tickets.Request(func(ticket *keboola.Ticket) {
			configWithRows.BranchID = branch.ID
			configWithRows.ID = keboola.ConfigID(ticket.ID)
			p.setEnv(fmt.Sprintf("%s_%s_ID", envPrefix, configFixture.Name), configWithRows.ID.String())
			p.logf("✔️ ID for config \"%s\".", configDesc)
		})

		// For each row
		for rowIndex, row := range configWithRows.Rows {
			rowIndex, row := rowIndex, row
			rowDesc := fmt.Sprintf("%s/%s", configDesc, row.Name)

			// Generate ID for row
			p.logf("▶ ID for config row \"%s\"...", rowDesc)
			tickets.Request(func(ticket *keboola.Ticket) {
				row.ID = keboola.RowID(ticket.ID)

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
			json.MustDecodeString(testhelper.MustReplaceEnvsString(json.MustEncodeString(configWithRows.Content, false), p.envs), &configWithRows.Content)
			for _, row := range configWithRows.Rows {
				json.MustDecodeString(testhelper.MustReplaceEnvsString(json.MustEncodeString(row.Content, false), p.envs), &row.Content)
			}

			// Send request
			_, err := p.keboolaAPIClient.
				CreateConfigRequest(configWithRows).
				WithBefore(func(ctx context.Context) error {
					p.logf("▶ Config \"%s/%s/%s\"...", branch.Name, configFixture.ComponentID, configFixture.Name)
					return nil
				}).
				WithOnSuccess(func(ctx context.Context, _ *keboola.ConfigWithRows) error {
					p.logf("✔️️ Config \"%s/%s/%s\"...", branch.Name, configFixture.ComponentID, configFixture.Name)
					if len(configFixture.Metadata) > 0 {
						_, err := p.keboolaAPIClient.
							AppendConfigMetadataRequest(configWithRows.ConfigKey, configFixture.Metadata).
							WithBefore(func(ctx context.Context) error {
								p.logf("▶ Config metadata \"%s\"...", configDesc)
								return nil
							}).
							WithOnSuccess(func(_ context.Context, _ client.NoResult) error {
								p.logf("✔️️ Config metadata \"%s\".", configDesc)
								return nil
							}).
							Send(ctx)
						return err
					}
					return nil
				}).
				Send(ctx)
			return err
		})
	}
}

func (p *Project) addBranch(branch *keboola.Branch) {
	p.setEnv(fmt.Sprintf("TEST_BRANCH_%s_ID", branch.Name), branch.ID.String())
	p.mapsLock.Lock()
	defer p.mapsLock.Unlock()
	p.branchesByID[branch.ID] = branch
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
	if testhelper.TestIsVerbose() && p.logFn != nil {
		p.logFn(format, a...)
	}
}
