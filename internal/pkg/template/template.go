package template

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/mountfs"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/project"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/template/context/load"
	templateInput "github.com/keboola/keboola-as-code/internal/pkg/template/input"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

const (
	ExpectedOutDirectory = "expected-out"
	IDRegexp             = `^[a-zA-Z0-9\-]+$`
	InputsFile           = "inputs.json"
	LongDescriptionFile  = "description.md"
	ReadmeFile           = "README.md"
	SrcDirectory         = "src"
	TestsDirectory       = "tests"
	InstanceIDForTest    = "instance-id"
)

type (
	Manifest     = templateManifest.Manifest
	ManifestFile = templateManifest.File
	Input        = templateInput.Input
	Inputs       = templateInput.Inputs
	InputValue   = templateInput.Value
	InputsValues = templateInput.Values
	StepsGroups  = templateInput.StepsGroups
	Steps        = templateInput.Steps
)

func ManifestPath() string {
	return templateManifest.Path()
}

func NewManifest() *Manifest {
	return templateManifest.New()
}

func LoadManifest(ctx context.Context, fs filesystem.Fs) (*ManifestFile, error) {
	return templateManifest.Load(ctx, fs)
}

func NewInputs() *Inputs {
	return templateInput.NewInputs()
}

func LoadInputs(ctx context.Context, fs filesystem.Fs, jsonnetCtx *jsonnet.Context) (StepsGroups, error) {
	return templateInput.Load(ctx, fs, jsonnetCtx)
}

func LoadLongDesc(ctx context.Context, fs filesystem.Fs) (string, error) {
	path := filesystem.Join(SrcDirectory, LongDescriptionFile)
	if !fs.Exists(ctx, path) {
		return "", nil
	}
	file, err := fs.ReadFile(ctx, filesystem.NewFileDef(path).SetDescription("template extended description"))
	if err != nil {
		return "", err
	}
	return file.Content, nil
}

func LoadReadme(ctx context.Context, fs filesystem.Fs) (string, error) {
	path := filesystem.Join(SrcDirectory, ReadmeFile)
	file, err := fs.ReadFile(ctx, filesystem.NewFileDef(path).SetDescription("template readme"))
	if err != nil {
		return "", err
	}
	return file.Content, nil
}

func ParseInputValue(ctx context.Context, value any, inputDef *templateInput.Input, isFilled bool) (InputValue, error) {
	// Convert
	value, err := inputDef.Type.ParseValue(value)
	if err != nil {
		return InputValue{}, errors.Errorf("invalid template input: %w", err)
	}

	// Validate all except oauth inputs
	if isFilled && inputDef.Kind != templateInput.KindOAuth && inputDef.Kind != templateInput.KindOAuthAccounts {
		if err := inputDef.ValidateUserInput(ctx, value); err != nil {
			return InputValue{}, errors.Errorf("invalid template input: %w", err)
		}
	}

	return InputValue{ID: inputDef.ID, Value: value, Skipped: !isFilled}, nil
}

type dependencies interface {
	Components() *model.ComponentsMap
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

type _reference = model.TemplateRef

type Template struct {
	_reference
	template         repository.TemplateRecord
	version          repository.VersionRecord
	deps             dependencies
	fs               filesystem.Fs
	srcDir           filesystem.Fs
	testsDir         filesystem.Fs
	projectsFilePath string
	longDescription  string
	readme           string
	manifestFile     *ManifestFile
	inputs           StepsGroups
}

type Test struct {
	tmpl *Template
	name string
	fs   filesystem.Fs
}

type CreatedTest struct {
	*Test
}

func New(ctx context.Context, reference model.TemplateRef, template repository.TemplateRecord, version repository.VersionRecord, templateDir, commonDir filesystem.Fs, projectsFilePath string, components *model.ComponentsMap) (*Template, error) {
	// Mount <common> directory to:
	//   template dir FS - used to load manifest, inputs, readme
	//   src dir FS - objects root
	mountPoint := mountfs.NewMountPoint(repository.CommonDirectoryMountPoint, commonDir)
	templateDir, err := aferofs.NewMountFs(templateDir, []mountfs.MountPoint{mountPoint})
	if err != nil {
		return nil, err
	}
	srcDir, err := templateDir.SubDirFs(SrcDirectory)
	if err != nil {
		return nil, err
	}
	srcDir, err = aferofs.NewMountFs(srcDir, []mountfs.MountPoint{mountPoint})
	if err != nil {
		return nil, err
	}

	// Create struct
	out := &Template{_reference: reference, template: template, version: version, fs: templateDir, srcDir: srcDir, projectsFilePath: projectsFilePath}

	// Create load context
	loadCtx := load.NewContext(ctx, srcDir, components)

	// Load manifest
	out.manifestFile, err = LoadManifest(ctx, templateDir)
	if err != nil {
		return nil, err
	}

	// Load inputs
	out.inputs, err = LoadInputs(ctx, templateDir, loadCtx.JsonnetContext())
	if err != nil {
		return nil, err
	}

	// Load long description
	out.longDescription, err = LoadLongDesc(ctx, templateDir)
	if err != nil {
		return nil, err
	}

	// Load readme
	out.readme, err = LoadReadme(ctx, templateDir)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (t *Template) Reference() model.TemplateRef {
	return t._reference
}

func (t *Template) TemplateRecord() repository.TemplateRecord {
	return t.template
}

func (t *Template) VersionRecord() repository.VersionRecord {
	return t.version
}

func (t *Template) ObjectsRoot() filesystem.Fs {
	return t.srcDir
}

func (t *Template) Fs() filesystem.Fs {
	return t.fs
}

func (t *Template) SrcDir() filesystem.Fs {
	return t.srcDir
}

func (t *Template) ProjectsFilePath() string {
	return t.projectsFilePath
}

func (t *Template) TestsDir(ctx context.Context) (filesystem.Fs, error) {
	if t.testsDir == nil {
		if !t.fs.IsDir(ctx, TestsDirectory) {
			err := t.fs.Mkdir(ctx, TestsDirectory)
			if err != nil {
				return nil, err
			}
		}

		testDir, err := t.fs.SubDirFs(TestsDirectory)
		if err == nil {
			t.testsDir = testDir
		} else {
			return nil, err
		}
	}

	return t.testsDir, nil
}

func (t *Template) Tests(ctx context.Context) (res []*Test, err error) {
	testsDir, err := t.TestsDir(ctx)
	if err != nil {
		return nil, err
	}

	// List sub files/directories
	paths, err := testsDir.Glob(ctx, "*")
	if err != nil {
		return nil, err
	}

	// Each subdirectory is a test
	for _, testName := range paths {
		if testsDir.IsDir(ctx, testName) {
			test, err := t.Test(ctx, testName)
			if err != nil {
				return nil, err
			}
			res = append(res, test)
		}
	}

	return res, nil
}

func (t *Template) Test(ctx context.Context, testName string) (*Test, error) {
	testsDir, err := t.TestsDir(ctx)
	if err != nil {
		return nil, err
	}

	if !testsDir.IsDir(ctx, testName) {
		return nil, errors.Errorf(`test "%s" not found in template "%s"`, testName, t.FullName())
	}

	testDir, err := testsDir.SubDirFs(testName)
	if err != nil {
		return nil, err
	}

	return &Test{name: testName, tmpl: t, fs: testDir}, nil
}

func (t *Template) CreateTest(ctx context.Context, testName string, inputsValues InputsValues, prjState *project.State, tmplInst string) error {
	testsDir, err := t.TestsDir(ctx)
	if err != nil {
		return err
	}

	if !testsDir.IsDir(ctx, testName) {
		err = testsDir.Mkdir(ctx, testName)
		if err != nil {
			return err
		}
	}

	testDir, err := testsDir.SubDirFs(testName)
	if err != nil {
		return err
	}

	test := &CreatedTest{Test: &Test{name: testName, tmpl: t, fs: testDir}}

	// Save gathered inputs to the template test inputs.json
	err = test.saveInputs(ctx, inputsValues)
	if err != nil {
		return err
	}

	// Save output from use template operation to the template test
	return test.saveExpectedOutput(ctx, prjState, tmplInst)
}

func (t *Template) LongDesc() string {
	return t.longDescription
}

func (t *Template) Readme() string {
	return t.readme
}

func (t *Template) Inputs() StepsGroups {
	return t.inputs
}

func (t *Template) Components() []string {
	return t.version.Components
}

func (t *Template) ManifestPath() string {
	return templateManifest.Path()
}

func (t *Template) ManifestExists(ctx context.Context) (bool, error) {
	return t.srcDir.IsFile(ctx, t.ManifestPath()), nil
}

func (t *Template) LoadState(templateCtx Context, options loadState.Options, d dependencies) (*State, error) {
	t.deps = d
	localFilter := templateCtx.LocalObjectsFilter()
	remoteFilter := templateCtx.RemoteObjectsFilter()
	loadOptions := loadState.OptionsWithFilter{
		Options:      options,
		LocalFilter:  &localFilter,
		RemoteFilter: &remoteFilter,
	}

	// Evaluate manifest
	container, err := t.evaluate(templateCtx)
	if err != nil {
		return nil, err
	}

	// Load state
	if s, err := loadState.Run(templateCtx, container, loadOptions, t.deps); err == nil {
		return NewState(s, container), nil
	} else {
		return nil, err
	}
}

func (t *Template) evaluate(templateCtx Context) (tmpl *evaluatedTemplate, err error) {
	_, span := t.deps.Telemetry().Tracer().Start(templateCtx, "keboola.go.declarative.template.evaluate")
	defer span.End(&err)

	// Evaluate manifest
	evaluatedManifest, err := t.manifestFile.Evaluate(templateCtx, templateCtx.JsonnetContext())
	if err != nil {
		return nil, err
	}

	return &evaluatedTemplate{
		Template: t,
		context:  templateCtx,
		manifest: evaluatedManifest,
	}, nil
}

type evaluatedTemplate struct {
	*Template
	context  Context
	manifest *Manifest
}

func (c *evaluatedTemplate) Manifest() manifest.Manifest {
	return c.manifest
}

func (c *evaluatedTemplate) TemplateManifest() *Manifest {
	return c.manifest
}

func (c *evaluatedTemplate) TemplateCtx() Context {
	return c.context
}

func (c *evaluatedTemplate) Ctx() context.Context {
	return c.context
}

func (c *evaluatedTemplate) MappersFor(state *state.State) (mapper.Mappers, error) {
	return MappersFor(state, c.deps, c.context)
}

func (c *evaluatedTemplate) MainConfig() (*model.TemplateMainConfig, error) {
	r, err := c.context.Replacements()
	if err != nil {
		return nil, err
	}

	// Replace ticket placeholder
	mainConfigRaw, err := r.Replace(c.manifest.MainConfig())
	if err != nil {
		return nil, err
	}
	mainConfig := mainConfigRaw.(*model.ConfigKey)
	if mainConfig == nil {
		return nil, nil
	}
	return &model.TemplateMainConfig{ConfigID: mainConfig.ID, ComponentID: mainConfig.ComponentID}, nil
}

func (t *Test) Name() string {
	return t.name
}

func (t *Test) ExpectedOutDir(ctx context.Context) (filesystem.Fs, error) {
	if !t.fs.IsDir(ctx, ExpectedOutDirectory) {
		return nil, errors.Errorf(`directory "%s" in test "%s" not found`, ExpectedOutDirectory, t.name)
	}

	// Get expected output dir
	originalFs, err := t.fs.SubDirFs(ExpectedOutDirectory)
	if err != nil {
		return nil, err
	}

	// Copy FS, so original dir is immutable
	copyFs := aferofs.NewMemoryFs()
	if err := aferofs.CopyFs2Fs(originalFs, "", copyFs, ""); err != nil {
		return nil, err
	}

	return copyFs, nil
}

func (t *Test) Inputs(ctx context.Context, provider testhelper.EnvProvider, replaceEnvsFn func(string, testhelper.EnvProvider, string) (string, error), envSeparator string) (map[string]any, error) {
	// Read inputs file
	file, err := t.fs.ReadFile(ctx, filesystem.NewFileDef(InputsFile).SetDescription("template inputs"))
	if err != nil {
		return nil, err
	}

	// Replace envs
	inputs := map[string]any{}
	if replaceEnvsFn != nil {
		file.Content, err = replaceEnvsFn(file.Content, provider, envSeparator)
		if err != nil {
			return nil, err
		}
	}

	// Decode JSON
	if err := json.DecodeString(file.Content, &inputs); err != nil {
		return nil, errors.Errorf(`cannot decode test inputs file "%s": %w`, InputsFile, err)
	}

	return inputs, nil
}

func (t *CreatedTest) saveInputs(ctx context.Context, inputsValues InputsValues) error {
	res := make(map[string]any)
	for k, v := range inputsValues.ToMap() {
		res[k] = v.Value
	}

	// Convert to Json
	jsonContent, err := json.EncodeString(res, true)
	if err != nil {
		return err
	}

	// Write file
	f := filesystem.NewRawFile(InputsFile, jsonContent)
	return t.fs.WriteFile(ctx, f)
}

func (t *CreatedTest) saveExpectedOutput(ctx context.Context, prjState *project.State, tmplInst string) error {
	// Get expected output dir
	if !t.fs.IsDir(ctx, ExpectedOutDirectory) {
		err := t.fs.Mkdir(ctx, ExpectedOutDirectory)
		if err != nil {
			return err
		}
	}

	expectedFS, err := t.fs.SubDirFs(ExpectedOutDirectory)
	if err != nil {
		return err
	}

	// Replace real IDs for placeholders
	err = replacePlaceholdersInManifest(ctx, prjState, tmplInst)
	if err != nil {
		return err
	}

	return aferofs.CopyFs2Fs(prjState.Fs(), "/", expectedFS, "/")
}

func replacePlaceholdersInManifest(ctx context.Context, prjState *project.State, tmplInst string) error {
	file, err := prjState.Fs().ReadFile(ctx, filesystem.NewFileDef(".keboola/manifest.json"))
	if err != nil {
		return err
	}
	file.Content = regexpcache.
		MustCompile(fmt.Sprintf(`"(?m)project": {\n    "id": %d,`, prjState.ProjectManifest().ProjectID())).
		ReplaceAllString(file.Content, "\"project\": {\n    \"id\": __PROJECT_ID__,")
	file.Content = regexpcache.
		MustCompile(fmt.Sprintf(`"apiHost": "%s"`, prjState.ProjectManifest().APIHost())).
		ReplaceAllString(file.Content, `"apiHost": "__STORAGE_API_HOST__"`)
	file.Content = regexpcache.
		MustCompile(fmt.Sprintf(`"(?m)branches": \[\n    {\n      "id": %s`, prjState.MainBranch().ID.String())).
		ReplaceAllString(file.Content, "\"branches\": [\n    {\n      \"id\": __MAIN_BRANCH_ID__")
	file.Content = regexpcache.
		MustCompile(fmt.Sprintf(`"branchId": %s`, prjState.MainBranch().ID.String())).
		ReplaceAllString(file.Content, `"branchId": __MAIN_BRANCH_ID__`)
	file.Content = regexpcache.
		MustCompile(tmplInst).
		ReplaceAllString(file.Content, `%s`)
	file.Content = regexpcache.
		MustCompile(`\\"date\\":\\"[^\\"]+\\"`).
		ReplaceAllString(file.Content, `\"date\":\"%s\"`)
	file.Content = regexpcache.
		MustCompile(`\\"tokenId\\":\\"\d+\\"`).
		ReplaceAllString(file.Content, `\"tokenId\":\"%s\"`)
	file.Content = regexpcache.
		MustCompile(`\\"configId\\":\\"\d+\\"`).
		ReplaceAllString(file.Content, `\"configId\":\"%s\"`)
	file.Content = regexpcache.
		MustCompile(`"id": "\d+"`).
		ReplaceAllString(file.Content, `"id": "%s"`)
	file.Content = regexpcache.
		MustCompile(`\\"idInProject\\":\\"\d+\\"`).
		ReplaceAllString(file.Content, `\"idInProject\":\"%s\"`)
	file.Content = regexpcache.
		MustCompile(`\\"rowId\\":\\"\d+\\"`).
		ReplaceAllString(file.Content, `\"rowId\":\"%s\"`)

	return prjState.Fs().WriteFile(ctx, file)
}
