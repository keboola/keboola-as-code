package run

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/go-utils/pkg/deepcopy"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	tmplTest "github.com/keboola/keboola-as-code/internal/pkg/template/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
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
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

func Run(ctx context.Context, tmpl *template.Template, o Options, d dependencies) (err error) {
	ctx, span := d.Telemetry().Tracer().Start(ctx, "keboola.go.operation.template.test.run")
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
		return errors.Errorf(`error running tests for template "%s": %w`, tmpl.TemplateID(), err)
	}

	errs := errors.NewMultiError()
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
				errs.AppendWithPrefixf(err, `running local test "%s" for template "%s" failed`, test.Name(), tmpl.TemplateID())
			} else {
				d.Logger().Infof(`PASS %s %s local`, tmpl.FullName(), test.Name())
			}
		}

		if !o.LocalOnly {
			if o.Verbose {
				d.Logger().Infof(`%s %s remote running`, tmpl.FullName(), test.Name())
			}
			if err := runRemoteTest(ctx, test, tmpl, o.Verbose, d); err != nil {
				d.Logger().Errorf(`FAIL %s %s remote`, tmpl.FullName(), test.Name())
				errs.AppendWithPrefixf(err, `running remote test "%s" for template "%s" failed`, test.Name(), tmpl.TemplateID())
			} else {
				d.Logger().Infof(`PASS %s %s remote`, tmpl.FullName(), test.Name())
			}
		}
	}

	return errs.ErrorOrNil()
}

func runLocalTest(ctx context.Context, test *template.Test, tmpl *template.Template, verbose bool, d dependencies) error {
	branchID := 1

	var logger log.Logger
	if verbose {
		logger = d.Logger()
	} else {
		logger = log.NewNopLogger()
	}

	prjState, testPrj, testDeps, unlockFn, err := tmplTest.PrepareProject(ctx, logger, d.Telemetry(), branchID, false)
	if err != nil {
		return err
	}
	defer unlockFn()
	d.Logger().Debugf(`Working directory set up.`)

	// Read inputs and replace env vars
	inputValues, err := tmplTest.ReadInputValues(ctx, tmpl, test)
	if err != nil {
		return err
	}
	d.Logger().Debugf(`Inputs prepared.`)

	// Use template
	tmplOpts := useTemplate.Options{
		InstanceName: "test",
		TargetBranch: model.BranchKey{ID: keboola.BranchID(branchID)},
		Inputs:       inputValues,
		InstanceID:   template.InstanceIDForTest,
		SkipEncrypt:  true,
	}
	_, err = useTemplate.Run(ctx, prjState, tmpl, tmplOpts, testDeps)
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
	testhelper.MustReplaceEnvsDir(prjState.Fs(), `/`, envProvider)
	testhelper.MustReplaceEnvsDirWithSeparator(expectedDirFs, `/`, envProvider, "__")
	// Replace secrets from env vars
	osEnvs, err := env.FromOs()
	if err != nil {
		return err
	}
	testhelper.MustReplaceEnvsDirWithSeparator(expectedDirFs, `/`, osEnvs, "##")

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

	prjState, testPrj, testDeps, unlockFn, err := tmplTest.PrepareProject(ctx, logger, d.Telemetry(), 0, true)
	if err != nil {
		return err
	}
	defer unlockFn()
	d.Logger().Debugf(`Working directory set up.`)

	branchKey := prjState.MainBranch().BranchKey

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
	opResult, err := useTemplate.Run(ctx, prjState, tmpl, tmplOpts, testDeps)
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
	replaceEnvs.Set("MAIN_BRANCH_ID", prjState.MainBranch().ID.String())
	envProvider := storageenvmock.CreateStorageEnvMockTicketProvider(ctx, replaceEnvs)
	testhelper.MustReplaceEnvsDir(prjState.Fs(), `/`, envProvider)
	testhelper.MustReplaceEnvsDirWithSeparator(expectedDirFs, `/`, envProvider, "__")
	// Replace secrets from env vars
	osEnvs, err := env.FromOs()
	if err != nil {
		return err
	}
	testhelper.MustReplaceEnvsDirWithSeparator(expectedDirFs, `/`, osEnvs, "##")

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
	tmplInst, err := findTmplInst(prjState, branchKey, opResult.InstanceID)
	if err != nil {
		return err
	}

	// Run the mainConfig job
	api := testPrj.KeboolaProjectAPI()
	job, err := api.NewCreateJobRequest(tmplInst.MainConfig.ComponentID).WithConfig(tmplInst.MainConfig.ConfigID).Send(ctx)
	if err != nil {
		return err
	}

	timeoutCtx, cancelFn := context.WithTimeout(ctx, time.Minute*10)
	defer cancelFn()
	return api.WaitForQueueJob(timeoutCtx, job.ID)
}

func reloadPrjState(ctx context.Context, prjState *project.State) error {
	ok, localErr, remoteErr := prjState.Load(ctx, state.LoadOptions{LoadRemoteState: true})
	if remoteErr != nil {
		return errors.Errorf(`state reload failed on remote error: %w`, remoteErr)
	}
	if localErr != nil {
		return errors.Errorf(`state reload failed on local error: %w`, localErr)
	}
	if !ok {
		return errors.New(`state reload failed`)
	}
	return nil
}

func findTmplInst(prjState *project.State, branchKey model.BranchKey, tmplInstID string) (*model.TemplateInstance, error) {
	branch, found := prjState.GetOrNil(branchKey).(*model.BranchState)
	if !found {
		return nil, errors.Errorf(`branch "%d" not found`, branchKey.ID)
	}
	tmplInst, found, err := branch.Remote.Metadata.TemplateInstance(tmplInstID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errors.Errorf(`template instance "%s" not found in branch metadata`, tmplInstID)
	}
	if tmplInst.MainConfig == nil {
		return nil, errors.Errorf(`template instance "%s" is missing mainConfig in metadata`, tmplInstID)
	}
	if tmplInst.MainConfig.ComponentID == "" {
		return nil, errors.Errorf(`template instance "%s" is missing mainConfig.componentId in metadata`, tmplInstID)
	}
	if tmplInst.MainConfig.ConfigID == "" {
		return nil, errors.Errorf(`template instance "%s" is missing mainConfig.configId in metadata`, tmplInstID)
	}
	return tmplInst, nil
}
