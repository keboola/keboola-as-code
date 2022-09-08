package run

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/keboola/go-client/pkg/jobsqueueapi"
	"github.com/keboola/go-client/pkg/storageapi"
	"github.com/keboola/go-utils/pkg/deepcopy"
	"go.opentelemetry.io/otel/trace"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	tmplTest "github.com/keboola/keboola-as-code/internal/pkg/template/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenvmock"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	syncPush "github.com/keboola/keboola-as-code/pkg/lib/operation/project/sync/push"
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
	ctx, span := d.Tracer().Start(ctx, "kac.lib.operation.template.test.run")
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
	branchID := 1

	var logger log.Logger
	if verbose {
		logger = d.Logger()
	} else {
		logger = log.NewNopLogger()
	}

	prjState, testPrj, testDeps, unlockFn, err := tmplTest.PrepareProject(ctx, d.Tracer(), logger, branchID, false)
	if err != nil {
		return err
	}
	defer unlockFn()
	d.Logger().Debugf(`Working directory set up.`)

	// Re-init template with set-up Storage client
	tmpl, err = testDeps.Template(ctx, tmpl.Reference())
	if err != nil {
		return err
	}

	// Read inputs and replace env vars
	inputValues, err := tmplTest.ReadInputValues(ctx, tmpl, test)
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
	if err != nil {
		return err
	}

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
	testhelper.ReplaceEnvsDir(prjState.Fs(), `/`, envProvider)
	testhelper.ReplaceEnvsDirWithSeparator(expectedDirFs, `/`, envProvider, "__")

	// Compare actual and expected dirs
	return testhelper.DirectoryContentsSame(expectedDirFs, `/`, prjState.Fs(), `/`)
}

func runRemoteTest(ctx context.Context, test *template.Test, tmpl *template.Template, verbose bool, d dependencies) error {
	var logger log.Logger
	if verbose {
		logger = d.Logger()
	} else {
		logger = log.NewNopLogger()
	}

	prjState, testPrj, testDeps, unlockFn, err := tmplTest.PrepareProject(ctx, d.Tracer(), logger, 0, true)
	if err != nil {
		return err
	}
	defer unlockFn()
	d.Logger().Debugf(`Working directory set up.`)

	branchKey := prjState.MainBranch().BranchKey

	// Re-init template with set-up Storage client
	tmpl, err = testDeps.Template(ctx, tmpl.Reference())
	if err != nil {
		return err
	}

	// Read inputs and replace env vars
	inputValues, err := tmplTest.ReadInputValues(ctx, tmpl, test)
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
	if err != nil {
		return err
	}

	// Copy expected state and replace ENVs
	expectedDirFs, err := test.ExpectedOutDir()
	if err != nil {
		return err
	}
	replaceEnvs := env.Empty()
	replaceEnvs.Set("STORAGE_API_HOST", testPrj.StorageAPIHost())
	replaceEnvs.Set("PROJECT_ID", strconv.Itoa(testPrj.ID()))
	replaceEnvs.Set("MAIN_BRANCH_ID", prjState.MainBranch().Id.String())
	envProvider := storageenvmock.CreateStorageEnvMockTicketProvider(ctx, replaceEnvs)
	testhelper.ReplaceEnvsDir(prjState.Fs(), `/`, envProvider)
	testhelper.ReplaceEnvsDirWithSeparator(expectedDirFs, `/`, envProvider, "__")

	// Compare actual and expected dirs
	err = testhelper.DirectoryContentsSame(expectedDirFs, `/`, prjState.Fs(), `/`)
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
	err = reloadPrjState(prjState)
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

func reloadPrjState(prjState *project.State) error {
	ok, localErr, remoteErr := prjState.Load(state.LoadOptions{LoadRemoteState: true})
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
