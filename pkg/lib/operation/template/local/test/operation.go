package test

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"go.opentelemetry.io/otel/trace"

	dependenciesPkg "github.com/keboola/keboola-as-code/internal/pkg/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	fixtures "github.com/keboola/keboola-as-code/internal/pkg/fixtures/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenvmock"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/testtemplateinputs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	syncPush "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/push"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	LocalOnly  bool   // run local tests only
	RemoteOnly bool   // run remote tests only
	TestName   string // run only selected test
	Verbose    bool   // verbose output
}

type dependencies interface {
	Tracer() trace.Tracer
	Logger() log.Logger
}

func Run(ctx context.Context, tmpl *template.Template, o Options, d dependencies) (err error) {
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.template.test")
	defer telemetry.EndSpan(span, &err)

	tempDir, err := os.MkdirTemp("", "kac-test-template-") //nolint:forbidigo
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil { // nolint: forbidigo
			d.Logger().Warnf(`cannot remove temp dir "%s": %w`, tempDir, err)
		}
	}()

	// Run through all tests
	tests, err := tmpl.Tests()
	if err != nil {
		return err
	}

	errors := utils.NewMultiError()
	for _, test := range tests {
		// Run only a single test?
		if o.TestName != "" && o.TestName != test.Name() {
			continue
		}

		if !o.RemoteOnly {
			if o.Verbose {
				d.Logger().Infof(`%s %s local running`, tmpl.FullName(), test.Name())
			}
			if err := runLocalTest(ctx, test, tmpl, o.Verbose, d); err != nil {
				d.Logger().Errorf(`FAIL %s %s local`, tmpl.FullName(), test.Name())
				errors.Append(fmt.Errorf(`running local test "%s" for template "%s" failed: %w`, test.Name(), tmpl.TemplateId(), err))
			} else {
				d.Logger().Infof(`PASS %s %s local`, tmpl.FullName(), test.Name())
			}
		}

		if !o.LocalOnly {
			if o.Verbose {
				d.Logger().Infof(`%s %s remote running`, tmpl.FullName(), test)
			}
			if err := runRemoteTest(ctx, test, tmpl, o.Verbose, d); err != nil {
				d.Logger().Errorf(`FAIL %s %s remote`, tmpl.FullName(), test.Name())
				errors.Append(fmt.Errorf(`running remote test "%s" for template "%s" failed: %w`, test.Name(), tmpl.TemplateId(), err))
			} else {
				d.Logger().Infof(`PASS %s %s remote`, tmpl.FullName(), test.Name())
			}
		}
	}

	return errors.ErrorOrNil()
}

func runLocalTest(ctx context.Context, test *template.Test, tmpl *template.Template, verbose bool, d dependencies) error {
	// Get OS envs
	envs, err := env.FromOs()
	if err != nil {
		return err
	}

	// Get a test project
	testPrj, unlockFn, err := testproject.GetTestProject(envs)
	if err != nil {
		return err
	}
	defer unlockFn()

	branchID := 1

	// Load fixture with minimal project
	fixPrjEnvs := env.Empty()
	fixPrjEnvs.Set("TEST_KBC_STORAGE_API_HOST", testPrj.StorageAPIHost())
	fixPrjEnvs.Set("LOCAL_PROJECT_ID", strconv.Itoa(testPrj.ID()))
	fixPrjEnvs.Set("LOCAL_STATE_MAIN_BRANCH_ID", strconv.Itoa(branchID))
	projectFS, err := fixtures.LoadFS("empty-branch", fixPrjEnvs)
	if err != nil {
		return err
	}

	var logger log.Logger
	if verbose {
		logger = d.Logger()
	} else {
		logger = log.NewNopLogger()
	}
	testDeps, err := newTestDependencies(ctx, d.Tracer(), logger, testPrj.StorageAPIHost(), testPrj.StorageAPIToken().Token)
	if err != nil {
		return err
	}

	// Re-init template with set-up Storage client
	tmpl, err = testDeps.Template(ctx, tmpl.Reference())
	if err != nil {
		return err
	}

	// Load project state
	prj, err := project.New(ctx, projectFS, true)
	if err != nil {
		return err
	}
	prjState, err := prj.LoadState(loadState.LocalOperationOptions(), testDeps)
	if err != nil {
		return err
	}
	d.Logger().Debugf(`Working directory set up.`)

	// Read inputs and replace env vars
	envInputsProvider, err := testtemplateinputs.CreateTestInputsEnvProvider(ctx)
	if err != nil {
		return err
	}
	inputsFile, err := test.Inputs(envInputsProvider, testhelper.ReplaceEnvsStringWithSeparator, "##")
	if err != nil {
		return err
	}

	inputValues := make(template.InputsValues, 0)
	err = tmpl.Inputs().ToExtended().VisitInputs(func(group *input.StepsGroupExt, step *input.StepExt, inputDef *input.Input) error {
		var inputValue template.InputValue
		if v, found := inputsFile[inputDef.Id]; found {
			inputValue, err = template.ParseInputValue(v, inputDef, true)
			if err != nil {
				return utils.PrefixError(err.Error(), fmt.Errorf("please fix the value in the inputs JSON file"))
			}
		} else {
			inputValue, err = template.ParseInputValue(inputDef.DefaultOrEmpty(), inputDef, true)
			if err != nil {
				return utils.PrefixError(err.Error(), fmt.Errorf("please define value in the inputs JSON file"))
			}
		}
		inputValues = append(inputValues, inputValue)
		return nil
	})
	if err != nil {
		return err
	}
	d.Logger().Debugf(`Inputs prepared.`)

	// Use template
	tmplOpts := useTemplate.Options{
		InstanceName: "test",
		TargetBranch: model.BranchKey{Id: storageapi.BranchID(branchID)},
		Inputs:       inputValues,
	}
	_, _, err = useTemplate.Run(ctx, prjState, tmpl, tmplOpts, testDeps)

	// Copy expected state and replace ENVs
	expectedDirFs, err := test.ExpectedOutDir()
	if err != nil {
		return err
	}
	replaceEnvs := env.Empty()
	replaceEnvs.Set("STORAGE_API_HOST", testPrj.StorageAPIHost())
	replaceEnvs.Set("PROJECT_ID", strconv.Itoa(testPrj.ID()))
	replaceEnvs.Set("MAIN_BRANCH_ID", strconv.Itoa(branchID))
	envProvider := storageenvmock.CreateStorageEnvMockTicketProvider(ctx, replaceEnvs)
	testhelper.ReplaceEnvsDir(projectFS, `/`, envProvider)
	testhelper.ReplaceEnvsDirWithSeparator(expectedDirFs, `/`, envProvider, "__")

	// Compare actual and expected dirs
	return testhelper.DirectoryContentsSame(expectedDirFs, `/`, projectFS, `/`)
}

func runRemoteTest(ctx context.Context, test *template.Test, tmpl *template.Template, verbose bool, d dependencies) error {
	// Get envs
	envs, err := env.FromOs()
	if err != nil {
		return err
	}

	// Get a test project
	testPrj, unlockFn, err := testproject.GetTestProject(envs)
	if err != nil {
		return err
	}
	defer unlockFn()

	// Clear project
	err = testPrj.SetState("empty.json")
	if err != nil {
		return err
	}

	defBranch, err := testPrj.DefaultBranch()
	if err != nil {
		return err
	}
	branchID := int(defBranch.ID)
	branchKey := model.BranchKey{Id: storageapi.BranchID(branchID)}

	// Load fixture with minimal project
	fixPrjEnvs := env.Empty()
	fixPrjEnvs.Set("TEST_KBC_STORAGE_API_HOST", testPrj.StorageAPIHost())
	fixPrjEnvs.Set("LOCAL_PROJECT_ID", strconv.Itoa(testPrj.ID()))
	fixPrjEnvs.Set("LOCAL_STATE_MAIN_BRANCH_ID", strconv.Itoa(branchID))
	prjFS, err := fixtures.LoadFS("empty-branch", fixPrjEnvs)
	if err != nil {
		return err
	}

	var logger log.Logger
	if verbose {
		logger = d.Logger()
	} else {
		logger = log.NewNopLogger()
	}
	testDeps, err := newTestDependencies(ctx, d.Tracer(), logger, testPrj.StorageAPIHost(), testPrj.StorageAPIToken().Token)
	if err != nil {
		return err
	}

	// Re-init template with set-up Storage client
	tmpl, err = testDeps.Template(ctx, tmpl.Reference())
	if err != nil {
		return err
	}

	// Load project state
	prj, err := project.New(ctx, prjFS, true)
	// Create fake manifest
	m := project.NewManifest(testPrj.ID(), testPrj.StorageAPIHost())
	// Load only target branch
	m.Filter().SetAllowedKeys([]model.Key{branchKey})

	if err != nil {
		return err
	}
	prjState, err := prj.LoadState(loadState.Options{LoadRemoteState: true}, testDeps)
	if err != nil {
		return err
	}
	d.Logger().Debugf(`Working directory set up.`)

	// Read inputs and replace env vars
	envInputsProvider, err := testtemplateinputs.CreateTestInputsEnvProvider(ctx)
	if err != nil {
		return err
	}
	inputsFile, err := test.Inputs(envInputsProvider, testhelper.ReplaceEnvsStringWithSeparator, "##")
	if err != nil {
		return err
	}

	inputValues := make(template.InputsValues, 0)
	err = tmpl.Inputs().ToExtended().VisitInputs(func(group *input.StepsGroupExt, step *input.StepExt, inputDef *input.Input) error {
		var inputValue template.InputValue
		if v, found := inputsFile[inputDef.Id]; found {
			inputValue, err = template.ParseInputValue(v, inputDef, true)
			if err != nil {
				return utils.PrefixError(err.Error(), fmt.Errorf("please fix the value in the inputs JSON file"))
			}
		} else {
			inputValue, err = template.ParseInputValue(inputDef.DefaultOrEmpty(), inputDef, true)
			if err != nil {
				return utils.PrefixError(err.Error(), fmt.Errorf("please define value in the inputs JSON file"))
			}
		}
		inputValues = append(inputValues, inputValue)
		return nil
	})
	if err != nil {
		return err
	}
	d.Logger().Debugf(`Inputs prepared.`)

	// Copy remote state to the local
	for _, objectState := range prjState.All() {
		objectState.SetLocalState(deepcopy.Copy(objectState.RemoteState()).(model.Object))
	}

	// Use template
	tmplOpts := useTemplate.Options{
		InstanceName: "test",
		TargetBranch: branchKey,
		Inputs:       inputValues,
	}
	tmplInstID, _, err := useTemplate.Run(ctx, prjState, tmpl, tmplOpts, testDeps)

	// Copy expected state and replace ENVs
	expectedDirFs, err := test.ExpectedOutDir()
	if err != nil {
		return err
	}
	replaceEnvs := env.Empty()
	replaceEnvs.Set("STORAGE_API_HOST", testPrj.StorageAPIHost())
	replaceEnvs.Set("PROJECT_ID", strconv.Itoa(testPrj.ID()))
	replaceEnvs.Set("MAIN_BRANCH_ID", strconv.Itoa(branchID))
	envProvider := storageenvmock.CreateStorageEnvMockTicketProvider(ctx, replaceEnvs)
	testhelper.ReplaceEnvsDir(prjFS, `/`, envProvider)
	testhelper.ReplaceEnvsDirWithSeparator(expectedDirFs, `/`, envProvider, "__")

	// Compare actual and expected dirs
	err = testhelper.DirectoryContentsSame(expectedDirFs, `/`, prjFS, `/`)
	if err != nil {
		return err
	}

	// E2E test
	// Push the project
	pushOpts := syncPush.Options{
		Encrypt:           true,
		DryRun:            false,
		SkipValidation:    false,
		AllowRemoteDelete: true,
		LogUntrackedPaths: true,
		ChangeDescription: "",
	}
	err = syncPush.Run(ctx, prjState, pushOpts, testDeps)
	if err != nil {
		return err
	}

	// Get mainConfig from applied template
	err = reloadPrjState(ctx, prjState)
	if err != nil {
		return err
	}
	tmplInst, err := findTmplInst(prjState, branchKey, tmplInstID)
	if err != nil {
		return err
	}

	// Run the mainConfig job
	queueClient := testPrj.JobsQueueAPIClient()
	job, err := jobsqueueapi.CreateJobRequest(tmplInst.MainConfig.ComponentId, tmplInst.MainConfig.ConfigId).Send(ctx, queueClient)
	if err != nil {
		return err
	}
	return jobsqueueapi.WaitForJob(ctx, queueClient, job)
}

func reloadPrjState(ctx context.Context, prjState *project.State) error {
	ok, localErr, remoteErr := prjState.Load(ctx, state.LoadOptions{LoadRemoteState: true})
	if remoteErr != nil {
		return fmt.Errorf(`state reload failed on remote error: %w`, remoteErr)
	}
	if localErr != nil {
		return fmt.Errorf(`state reload failed on local error: %w`, localErr)
	}
	if !ok {
		return fmt.Errorf(`state reload failed`)
	}
	return nil
}

func findTmplInst(prjState *project.State, branchKey model.BranchKey, tmplInstID string) (*model.TemplateInstance, error) {
	branch, found := prjState.GetOrNil(branchKey).(*model.BranchState)
	if !found {
		return nil, fmt.Errorf(`branch "%d" not found`, branchKey.Id)
	}
	tmplInst, found, err := branch.Remote.Metadata.TemplateInstance(tmplInstID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf(`template instance "%s" not found in branch metadata`, tmplInstID)
	}
	if tmplInst.MainConfig == nil {
		return nil, fmt.Errorf(`template instance "%s" is missing mainConfig in metadata`, tmplInstID)
	}
	if tmplInst.MainConfig.ComponentId == "" {
		return nil, fmt.Errorf(`template instance "%s" is missing mainConfig.componentId in metadata`, tmplInstID)
	}
	if tmplInst.MainConfig.ConfigId == "" {
		return nil, fmt.Errorf(`template instance "%s" is missing mainConfig.configId in metadata`, tmplInstID)
	}
	return tmplInst, nil
}

type testDependencies struct {
	dependenciesPkg.Base
	dependenciesPkg.Public
	dependenciesPkg.Project
}

func newTestDependencies(ctx context.Context, tracer trace.Tracer, logger log.Logger, apiHost, apiToken string) (*testDependencies, error) {
	baseDeps := dependenciesPkg.NewBaseDeps(env.Empty(), tracer, logger, client.NewTestClient())
	publicDeps, err := dependenciesPkg.NewPublicDeps(ctx, baseDeps, apiHost)
	if err != nil {
		return nil, err
	}
	projectDeps, err := dependenciesPkg.NewProjectDeps(ctx, baseDeps, publicDeps, apiToken)
	if err != nil {
		return nil, err
	}
	return &testDependencies{
		Base:    baseDeps,
		Public:  publicDeps,
		Project: projectDeps,
	}, nil
}
