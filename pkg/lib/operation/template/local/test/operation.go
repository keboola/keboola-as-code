package test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/storageapi"

	cliDeps "github.com/keboola/keboola-as-code/internal/pkg/cli/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/dialog"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/options"
	"github.com/keboola/keboola-as-code/internal/pkg/cli/prompt/nop"
	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	fixtures "github.com/keboola/keboola-as-code/internal/pkg/fixtures/local"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/template"
	"github.com/keboola/keboola-as-code/internal/pkg/template/input"
	"github.com/keboola/keboola-as-code/internal/pkg/utils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testfs"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper/storageenv"
	useTemplate "github.com/keboola/keboola-as-code/pkg/lib/operation/project/local/template/use"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Options struct {
	Path string
}

type dependencies interface {
	Components() (*model.ComponentsMap, error)
	Ctx() context.Context
	EncryptionApiClient() (client.Sender, error)
	HttpClient() client.Client
	Logger() log.Logger
	SchedulerApiClient() (client.Sender, error)
	ProjectID() (int, error)
	StorageApiHost() (string, error)
	StorageAPITokenID() (string, error)
	StorageApiClient() (client.Sender, error)
}

// getATestProject takes first project from TEST_KBC_PROJECTS env var.
//@TODO from github.com/keboola/go-utils/pkg/testproject/testproject.go:155.
func getATestProject() (string, string, string) {
	if def, found := os.LookupEnv(`TEST_KBC_PROJECTS`); found {
		// Each project definition is separated by ";"
		projects := strings.Split(def, ";")
		p := strings.TrimSpace(projects[0])

		// Definition format: storage_api_host|project_id|project_token
		parts := strings.Split(p, `|`)

		// Check number of parts
		if len(parts) != 3 {
			panic(fmt.Errorf(
				`project definition in TEST_PROJECTS env must be in "storage_api_host|project_id|project_token " format, given "%s"`,
				p,
			))
		}

		host := strings.TrimSpace(parts[0])
		id := strings.TrimSpace(parts[1])
		token := strings.TrimSpace(parts[2])
		return host, id, token
	}
	return "", "", ""
}

func Run(tmpl *template.Template, d dependencies) (err error) {
	tempDir, err := ioutil.TempDir("", "kac-test-template-")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil { // nolint: forbidigo
			d.Logger().Warnf(`cannot remove temp dir "%s": %w`, tempDir, err)
		}
	}()

	repoDirFS, err := prepareRepoFS(tempDir, tmpl)

	// Run through all tests
	testsList, err := tmpl.ListTests()
	if err != nil {
		return err
	}

	for _, testName := range testsList {
		if err := runSingleTest(testName, tmpl, repoDirFS, d); err != nil {
			return fmt.Errorf(`running test "%s" for template "%s" failed: %w`, testName, tmpl.TemplateId(), err)
		}
		d.Logger().Infof(`Test "%s" finished.`, testName)
	}

	return nil
}

func runSingleTest(testName string, tmpl *template.Template, repoDirFS filesystem.Fs, d dependencies) error {
	// Get a test project
	projectHost, projectId, projectToken := getATestProject()
	storageApiClient := storageapi.ClientWithHostAndToken(client.NewTestClient(), projectHost, projectToken)

	// Load fixture with minimal project
	fixProjectEnvs := env.Empty()
	fixProjectEnvs.Set("TEST_KBC_STORAGE_API_HOST", projectHost)
	fixProjectEnvs.Set("LOCAL_PROJECT_ID", projectId)
	fixProjectEnvs.Set("STORAGE_API_HOST", projectHost)
	fixProjectEnvs.Set("PROJECT_ID", projectId)
	projectFS, err := fixtures.LoadFS("empty-branch", fixProjectEnvs)
	if err != nil {
		return err
	}

	opts := options.New()
	opts.Set(`storage-api-host`, projectHost)
	opts.Set(`storage-api-token`, projectToken)
	tmplDeps := cliDeps.NewContainer(d.Ctx(), env.Empty(), repoDirFS, dialog.New(nop.New()), d.Logger(), opts)
	projectDeps := cliDeps.NewContainer(d.Ctx(), env.Empty(), projectFS, dialog.New(nop.New()), d.Logger(), opts)

	// Re-init template with set-up Storage client
	tmpl, err = tmplDeps.Template(tmpl.Reference())
	if err != nil {
		return err
	}

	// Load project state
	prj, err := project.New(projectFS, true, projectDeps)
	if err != nil {
		return err
	}
	prjState, err := prj.LoadState(loadState.LocalOperationOptions())
	if err != nil {
		return err
	}
	d.Logger().Debugf(`Working directory set up.`)

	// Read inputs
	inputsFile, err := tmpl.TestInputs(testName)
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
		TargetBranch: model.BranchKey{Id: 1},
		Inputs:       inputValues,
	}
	_, _, err = useTemplate.Run(prjState, tmpl, tmplOpts, projectDeps)

	// Copy expected state and replace ENVs
	expectedDirFs, err := tmpl.TestExpectedOutFS(testName)
	if err != nil {
		return err
	}
	envProvider := storageenv.CreateStorageEnvTicketProvider(d.Ctx(), storageApiClient, fixProjectEnvs)
	testhelper.ReplaceEnvsDir(projectFS, `/`, envProvider)
	testhelper.ReplaceEnvsDirWithSeparator(expectedDirFs, `/`, envProvider, "__")

	// Compare actual and expected dirs
	return testhelper.DirectoryContentsSame(expectedDirFs, `/`, projectFS, `/`)
}

func prepareRepoFS(tempDir string, tmpl *template.Template) (filesystem.Fs, error) {
	// Create virtual fs for working dir
	repoFS := testfs.NewBasePathLocalFs(tempDir)

	// Load fixture with minimal repository
	fixRepoEnvs := env.Empty()
	fixRepoEnvs.Set("TEMPLATE_ID", tmpl.TemplateId())
	fixRepoEnvs.Set("TEMPLATE_NAME", tmpl.FullName())
	fixRepoEnvs.Set("TEMPLATE_VERSION", tmpl.Version())
	fixRepoFS, err := fixtures.LoadFS("repository-basic", fixRepoEnvs)
	if err != nil {
		return nil, err
	}
	if err := aferofs.CopyFs2Fs(fixRepoFS, `/`, repoFS, `/`); err != nil {
		return nil, err
	}

	// Load the template dir
	if err := aferofs.CopyFs2Fs(tmpl.SrcDir(), `/`, repoFS, fmt.Sprintf(`/%s/%s/src`, tmpl.TemplateId(), tmpl.Version())); err != nil {
		return nil, err
	}
	testsDir, err := tmpl.TestsDir()
	if err != nil {
		return nil, err
	}
	if err := aferofs.CopyFs2Fs(testsDir, `/`, repoFS, fmt.Sprintf(`/%s/%s/tests`, tmpl.TemplateId(), tmpl.Version())); err != nil {
		return nil, err
	}

	return repoFS, nil
}
