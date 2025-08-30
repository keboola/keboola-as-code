package testproject

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/testproject"
	"github.com/keboola/keboola-sdk-go/v2/pkg/client"
	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/keboola/keboola-sdk-go/v2/pkg/request"
	"github.com/spf13/cast"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/fixtures"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ulid"
)

type Project struct {
	*testproject.Project
	initStartedAt     time.Time
	ctx               context.Context
	storageAPIToken   *keboola.Token
	keboolaProjectAPI *keboola.AuthorizedAPI
	defaultBranch     *keboola.Branch
	envs              *env.Map
	mapsLock          *sync.Mutex
	stateFilePath     string
	branchesByID      map[keboola.BranchID]*keboola.Branch
	branchesByName    map[string]*keboola.Branch
	logFn             func(format string, a ...any)
}

type UnlockFn func()

func GetTestProjectForTest(t *testing.T, path string, options ...testproject.Option) *Project {
	t.Helper()

	p, unlockFn, err := GetTestProject(path, env.Empty(), options...)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		// Unlock and cancel background jobs
		unlockFn()
	})

	p.logFn = func(format string, a ...any) {
		seconds := float64(time.Since(p.initStartedAt).Milliseconds()) / 1000
		a = append([]any{p.ID(), t.Name(), seconds}, a...)
		t.Logf("TestProject[%d][%s][%05.2fs]: "+format, a...)
	}

	return p
}

func GetTestProject(path string, envs *env.Map, options ...testproject.Option) (*Project, UnlockFn, error) {
	project, unlockFn, err := testproject.GetTestProjectInPath(path, options...)
	if err != nil {
		return nil, nil, err
	}

	ctx, cancelFn := context.WithCancelCause(context.Background()) // nolint: contextcheck
	p := &Project{Project: project, initStartedAt: time.Now(), ctx: ctx, mapsLock: &sync.Mutex{}}
	p.logf("□ Initializing project...")

	cleanupFn := func() {
		cancelFn(errors.New("test project cleanup"))
		unlockFn()
	}

	// Init storage API
	httpClient := client.NewTestClient()
	p.keboolaProjectAPI, err = keboola.NewAuthorizedAPI(ctx, p.StorageAPIHost(), p.Project.StorageAPIToken(), keboola.WithClient(&httpClient), keboola.WithOnSuccessTimeout(1*time.Minute))
	if err != nil {
		return nil, nil, err
	}
	// Check token/project ID
	errs := errors.NewMultiError()
	initWg := &sync.WaitGroup{}
	initWg.Add(1)
	go func() {
		defer initWg.Done()
		if token, err := p.keboolaProjectAPI.VerifyTokenRequest(p.Project.StorageAPIToken()).Send(p.ctx); err != nil {
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
	p.setEnv(`TEST_KBC_PROJECT_STAGING_STORAGE`, p.StagingStorage())
	p.setEnv(`TEST_KBC_STORAGE_API_HOST`, p.StorageAPIHost())
	p.setEnv(`TEST_KBC_MASTER_TOKEN`, p.Project.StorageAPIToken())
	p.setEnv(`TEST_KBC_PROJECT_BACKEND`, p.Backend())
	p.setEnv(`TEST_KBC_PROJECT_LEGACY_TRANSFORMATION`, strconv.FormatBool(p.LegacyTransformation()))
	p.logf(`■ ️Initialization done.`)

	// Remove all objects
	if err := p.Clean(); !p.IsGuest() && err != nil {
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
		if v, err := p.keboolaProjectAPI.GetDefaultBranchRequest().Send(p.ctx); err == nil {
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

func (p *Project) PublicAPI() *keboola.PublicAPI {
	return p.keboolaProjectAPI.PublicAPI
}

func (p *Project) ProjectAPI() *keboola.AuthorizedAPI {
	return p.keboolaProjectAPI
}

// Clean method deletes all project branches (except default), all configurations, all schedules, and all sandboxes.
// It also sets the project's default branch.
func (p *Project) Clean() error {
	p.logf("□ Cleaning project...")

	ctx, cancel := context.WithTimeoutCause(context.Background(), 5*time.Minute, errors.New("project clean timeout"))
	defer cancel()

	// Clean whole project - configs, buckets, schedules, sandbox instances, etc.
	if err := keboola.CleanProject(ctx, p.keboolaProjectAPI); err != nil {
		return errors.Errorf(`cannot clean project "%d": %w`, p.ID(), err)
	}

	defaultBranch, err := p.keboolaProjectAPI.GetDefaultBranchRequest().Send(ctx)
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

func (p *Project) SetState(ctx context.Context, fs filesystem.Fs, projectStateFile string) error {
	if p.stateFilePath != "" {
		return errors.New("SetState method can be called only once after the Clean method")
	}
	p.stateFilePath = filesystem.Join(fs.WorkingDir(), projectStateFile)

	// Set new state
	p.logf("□ Setting project state ...")

	// Log ENVs at the end
	defer p.logEnvs()

	if !fs.Exists(ctx, p.stateFilePath) {
		err := fs.WriteFile(ctx, filesystem.NewRawFile(p.stateFilePath, `
{
  "allBranchesConfigs": [],
  "branches": [
    {
      "branch": {
        "name": "Main",
        "isDefault": true
      }
    }
  ]
}`))
		if err != nil {
			return err
		}
	}

	// Load state file
	stateFile, err := fixtures.LoadStateFile(fs, p.stateFilePath)
	if err != nil {
		return err
	}

	// Create configs in default branch, they will be auto-copied to branches created later
	err = p.createConfigsInDefaultBranch(stateFile.AllBranchesConfigs)
	if err != nil {
		return err
	}

	// Create branches
	if !p.IsGuest() {
		err = p.createBranches(stateFile.Branches)
		if err != nil {
			return err
		}
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

	// Create files
	err = p.createFiles(stateFile.Files)
	if err != nil {
		return err
	}

	if !p.IsGuest() {
		// Create sandboxes in default branch
		err = p.createSandboxes(p.defaultBranch.ID, stateFile.Sandboxes)
		if err != nil {
			return err
		}
	}

	p.logf("■ Project state set.")
	return nil
}

func (p *Project) createBranches(branches []*fixtures.BranchState) error {
	ctx, cancelFn := context.WithCancelCause(context.Background())
	defer cancelFn(errors.New("branches creation cancelled"))

	// Only one create branch request can run simultaneously.
	// Branch deletion is performed via Storage Job, which uses locks.
	// If we ran multiple requests, then only one job would run and the other jobs would wait.
	// The problem is that the lock is checked again after 30 seconds, so there is a long delay.
	createBranchSem := semaphore.NewWeighted(1)

	// Create branches
	grp := request.NewWaitGroup(ctx)
	for _, fixture := range branches {
		grp.Send(p.createBranchRequest(fixture, createBranchSem))
	}

	// Wait for all requests
	if err := grp.Wait(); err != nil {
		return err
	}
	return nil
}

func (p *Project) createBucketsTables(buckets []*fixtures.Bucket) error {
	ctx, cancelFn := context.WithCancelCause(context.Background())
	defer cancelFn(errors.New("buckets tables creation cancelled"))

	// Create buckets and tables
	grp := request.NewWaitGroup(ctx)
	for _, b := range buckets {
		req := p.keboolaProjectAPI.
			CreateBucketRequest(&keboola.Bucket{
				BucketKey: keboola.BucketKey{
					BranchID: p.defaultBranch.ID,
					BucketID: b.ID,
				},
				Description: b.Description,
			}).
			WithBefore(func(ctx context.Context) error {
				p.logf("▶ Bucket \"%s.c-%s\"...", b.ID.Stage, b.ID.BucketName)
				return nil
			}).
			WithOnComplete(func(ctx context.Context, apiBucket *keboola.Bucket, err error) error {
				if err == nil {
					p.logf("✔️ Bucket \"%s\".", apiBucket.BucketID)

					for _, t := range b.Tables {
						tableKey := keboola.TableKey{BranchID: p.defaultBranch.ID, TableID: t.ID}
						if len(t.Rows) > 0 {
							p.logf("▶ Table (with rows) \"%s\"...", t.Name)

							fileName := fmt.Sprintf("%s.data", t.ID)
							p.logf("▶ Table \"%s\" file resource \"%s\"...", t.Name, fileName)
							file, err := p.keboolaProjectAPI.CreateFileResourceRequest(p.defaultBranch.ID, fileName).Send(ctx)
							if err != nil {
								return err
							}
							p.logf("✔️ Table \"%s\" file resource \"%s\".", t.Name, fileName)

							p.logf("▶ Upload file \"%s\"...", fileName)
							buf := bytes.NewBuffer([]byte{})
							w := csv.NewWriter(buf)
							err = w.Write(t.Columns)
							if err != nil {
								return err
							}
							err = w.WriteAll(t.Rows)
							if err != nil {
								return err
							}
							_, err = keboola.Upload(ctx, file, buf)
							if err != nil {
								return err
							}
							p.logf("✔️ Upload file \"%s\".", fileName)

							p.logf("▶ Table \"%s\" from file resource \"%s\"...", t.Name, fileName)
							_, err = p.keboolaProjectAPI.CreateTableFromFileRequest(tableKey, file.FileKey, keboola.WithPrimaryKey(t.PrimaryKey)).Send(ctx)
							if err != nil {
								return err
							}
							p.logf("✔️ Table (with rows) \"%s\"(%s).", t.Name, t.ID)
						} else {
							p.logf("▶ Table \"%s\"...", t.Name)
							_, err = p.keboolaProjectAPI.CreateTableRequest(tableKey, t.Columns, keboola.WithPrimaryKey(t.PrimaryKey)).Send(ctx)
							if err != nil {
								return err
							}
							p.logf("✔️ Table \"%s\"(%s).", t.Name, t.ID)
						}
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

func (p *Project) createFiles(files []*fixtures.File) error {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 5*time.Minute, errors.New("files creation timeout"))
	defer cancel()

	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()

	for _, fixture := range files {
		wg.Add(1)
		go func() {
			defer wg.Done()

			opts := make([]keboola.CreateFileOption, 0)
			opts = append(opts, keboola.WithIsPermanent(fixture.IsPermanent))
			opts = append(opts, keboola.WithIsSliced(fixture.IsSliced))
			opts = append(opts, keboola.WithTags(fixture.Tags...))

			p.logf("▶ File \"%s\"...", fixture.Name)
			file, err := p.keboolaProjectAPI.CreateFileResourceRequest(p.defaultBranch.ID, fixture.Name, opts...).Send(ctx)
			if err != nil {
				errs.Append(errors.Errorf("could not create file \"%s\": %w", fixture.Name, err))
				return
			}

			if fixture.Content != "" {
				_, err = keboola.Upload(ctx, file, strings.NewReader(fixture.Content))
				if err != nil {
					errs.Append(errors.Errorf("could not upload file \"%s\" content: %w", fixture.Name, err))
					return
				}
			}

			if len(fixture.Slices) > 0 {
				slices := make([]string, 0, len(fixture.Slices))
				for _, slice := range fixture.Slices {
					_, err = keboola.UploadSlice(ctx, file, slice.Name, strings.NewReader(slice.Content))
					if err != nil {
						errs.Append(errors.Errorf("could not upload file \"%s\" slice \"%s\": %w", fixture.Name, slice.Name, err))
						return
					}
					slices = append(slices, slice.Name)
				}
				_, err = keboola.UploadSlicedFileManifest(ctx, file, slices)
				if err != nil {
					errs.Append(errors.Errorf("could not upload file \"%s\" manifest: %w", fixture.Name, err))
					return
				}
			}

			p.logf("✔️ File \"%s\"(%s).", file.Name, file.FileID)
			p.setEnv(fmt.Sprintf("TEST_FILE_%s_ID", fixture.Name), file.FileID.String())
		}()
	}

	wg.Wait()
	if errs.Len() > 0 {
		return errs
	}

	return nil
}

func (p *Project) createSandboxes(defaultBranchID keboola.BranchID, sandboxes []*fixtures.Sandbox) error {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 5*time.Minute, errors.New("sandboxes creation timeout"))
	defer cancel()

	wg := &sync.WaitGroup{}
	errs := errors.NewMultiError()

	for _, fixture := range sandboxes {
		wg.Add(1)
		go func() {
			defer wg.Done()

			opts := make([]keboola.CreateWorkspaceOption, 0)
			if keboola.WorkspaceSupportsSizes(fixture.Type) && len(fixture.Size) > 0 {
				opts = append(opts, keboola.WithSize(fixture.Size))
			}

			p.logf("▶ Sandbox \"%s\"...", fixture.Name)
			sandbox, err := p.keboolaProjectAPI.CreateWorkspace(
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

func (p *Project) createBranchRequest(fixture *fixtures.BranchState, createBranchSem *semaphore.Weighted) request.APIRequest[*keboola.Branch] {
	var req request.APIRequest[*keboola.Branch]

	// Create branch
	if fixture.IsDefault {
		// Reset default branch description (default branch cannot be created/deleted)
		req = request.NewNoOperationAPIRequest(p.defaultBranch) // default branch already exists
		if p.defaultBranch.Description != fixture.Description {
			req = req.WithOnSuccess(func(ctx context.Context, branch *keboola.Branch) error {
				branch.Description = fixture.Description
				return p.keboolaProjectAPI.
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
		req = p.keboolaProjectAPI.
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
	req = req.WithOnSuccess(func(ctx context.Context, branch *keboola.Branch) error {
		p.addBranch(branch)
		return nil
	})

	// Set branch metadata
	req = req.WithOnSuccess(func(ctx context.Context, branch *keboola.Branch) error {
		return p.keboolaProjectAPI.
			AppendBranchMetadataRequest(branch.BranchKey, fixture.Metadata).
			WithBefore(func(ctx context.Context) error {
				p.logf("▶ Branch metadata \"%s\"...", fixture.Name)
				return nil
			}).
			WithOnComplete(func(ctx context.Context, _ request.NoResult, err error) error {
				if err == nil {
					p.logf("✔️ Branch metadata \"%s\".", fixture.Name)
					return nil
				} else {
					return errors.Errorf(`cannot set branch metadata: %w`, err)
				}
			}).
			SendOrErr(ctx)
	})
	return req
}

func (p *Project) createConfigsInDefaultBranch(configs []string) error {
	ctx, cancelFn := context.WithCancelCause(p.ctx)
	defer cancelFn(errors.New("configs creation in default branch cancelled"))

	grp, ctx := errgroup.WithContext(ctx) // group for all parallel requests
	sendReady := make(chan struct{})      // block requests until IDs and ENVs will be ready

	// Prepare configs
	envPrefix := "TEST_BRANCH_ALL_CONFIG"
	p.prepareConfigs(
		ctx,
		grp,
		sendReady,
		envPrefix,
		configs,
		p.defaultBranch,
	)

	// Wait for requests
	close(sendReady) // unblock requests
	if err := grp.Wait(); err != nil {
		return errors.Errorf("cannot create configs: %w", err)
	}
	return nil
}

func (p *Project) createConfigs(branches []*fixtures.BranchState, additionalEnvs map[string]string) error {
	ctx, cancelFn := context.WithCancelCause(p.ctx)
	defer cancelFn(errors.New("configs creation cancelled"))

	grp, ctx := errgroup.WithContext(ctx) // group for all parallel requests
	sendReady := make(chan struct{})      // block requests until IDs and ENVs will be ready

	// Prepare configs
	for _, branchFixture := range branches {
		envPrefix := fmt.Sprintf("TEST_BRANCH_%s_CONFIG", branchFixture.Name)
		p.prepareConfigs(
			ctx,
			grp,
			sendReady,
			envPrefix,
			branchFixture.Configs,
			p.branchesByName[branchFixture.Name],
		)
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

func (p *Project) prepareConfigs(
	ctx context.Context,
	grp *errgroup.Group,
	sendReady <-chan struct{},
	envPrefix string,
	names []string,
	branch *keboola.Branch,
) {
	generator := ulid.NewDefaultGenerator()
	for _, name := range names {
		configFixture := fixtures.LoadConfig(name)
		configWithRows := configFixture.ToAPI()
		configDesc := fmt.Sprintf("%s/%s/%s", branch.Name, configFixture.ComponentID, configFixture.Name)

		// Generate ID for config
		p.logf("▶ ID for config \"%s\"...", configDesc)
		// Generate ULID
		newID := generator.NewULID()
		configWithRows.BranchID = branch.ID
		configWithRows.ID = keboola.ConfigID(newID)
		p.setEnv(fmt.Sprintf("%s_%s_ID", envPrefix, configFixture.Name), newID)
		p.logf("✔️ ID for config \"%s\".", configDesc)

		// For each row
		for rowIndex, row := range configWithRows.Rows {
			rowDesc := fmt.Sprintf("%s/%s", configDesc, row.Name)

			// Generate ID for row
			p.logf("▶ ID for config row \"%s\"...", rowDesc)
			// Generate ULID
			newID := generator.NewULID()
			row.ID = keboola.RowID(newID)

			// Generate row name for ENV, if needed
			rowName := row.Name
			if len(rowName) == 0 {
				rowName = cast.ToString(rowIndex + 1)
			}
			p.setEnv(fmt.Sprintf("%s_%s_ROW_%s_ID", envPrefix, configFixture.Name, rowName), row.ID.String())
			p.logf("✔️ ID for config row \"%s\".", rowDesc)
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
			_, err := p.keboolaProjectAPI.
				CreateConfigRequest(configWithRows, true).
				WithBefore(func(ctx context.Context) error {
					p.logf("▶ Config \"%s/%s/%s\"...", branch.Name, configFixture.ComponentID, configFixture.Name)
					return nil
				}).
				WithOnSuccess(func(ctx context.Context, _ *keboola.ConfigWithRows) error {
					p.logf("✔️️ Config \"%s/%s/%s\"...", branch.Name, configFixture.ComponentID, configFixture.Name)
					if len(configFixture.Metadata) > 0 {
						_, err := p.keboolaProjectAPI.
							AppendConfigMetadataRequest(configWithRows.ConfigKey, configFixture.Metadata).
							WithBefore(func(ctx context.Context) error {
								p.logf("▶ Config metadata \"%s\"...", configDesc)
								return nil
							}).
							WithOnSuccess(func(_ context.Context, _ request.NoResult) error {
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

func (p *Project) logf(format string, a ...any) {
	if testhelper.TestIsVerbose() && p.logFn != nil {
		p.logFn(format, a...)
	}
}
