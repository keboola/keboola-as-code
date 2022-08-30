package template

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/aferofs/mountfs"
	"github.com/keboola/keboola-as-code/internal/pkg/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	templateInput "github.com/keboola/keboola-as-code/internal/pkg/template/input"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/template/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

const (
	ExpectedOutDirectory = "expected-out"
	IdRegexp             = `^[a-zA-Z0-9\-]+$`
	InputsFile           = "inputs.json"
	LongDescriptionFile  = "description.md"
	ReadmeFile           = "README.md"
	SrcDirectory         = "src"
	TestsDirectory       = "tests"
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

func LoadManifest(fs filesystem.Fs) (*ManifestFile, error) {
	return templateManifest.Load(fs)
}

func NewInputs() *Inputs {
	return templateInput.NewInputs()
}

func LoadInputs(fs filesystem.Fs) (StepsGroups, error) {
	return templateInput.Load(fs)
}

func LoadLongDesc(fs filesystem.Fs) (string, error) {
	path := filesystem.Join(SrcDirectory, LongDescriptionFile)
	if !fs.Exists(path) {
		return "", nil
	}
	file, err := fs.ReadFile(filesystem.NewFileDef(path).SetDescription("template extended description"))
	if err != nil {
		return "", err
	}
	return file.Content, nil
}

func LoadReadme(fs filesystem.Fs) (string, error) {
	path := filesystem.Join(SrcDirectory, ReadmeFile)
	file, err := fs.ReadFile(filesystem.NewFileDef(path).SetDescription("template readme"))
	if err != nil {
		return "", err
	}
	return file.Content, nil
}

func LoadTestInputs(fs filesystem.Fs, provider testhelper.EnvProvider, replaceEnvsFn func(string, testhelper.EnvProvider, string) string, envSeparator string) (map[string]interface{}, error) {
	file, err := fs.ReadFile(filesystem.NewFileDef(InputsFile).SetDescription("template inputs"))
	if err != nil {
		return nil, err
	}
	inputs := map[string]interface{}{}
	if replaceEnvsFn != nil {
		file.Content = replaceEnvsFn(file.Content, provider, envSeparator)
	}
	if err := json.DecodeString(file.Content, &inputs); err != nil {
		return nil, fmt.Errorf(`cannot decode test inputs file "%s": %w`, InputsFile, err)
	}

	return inputs, nil
}

func ParseInputValue(value interface{}, inputDef *templateInput.Input, isFilled bool) (InputValue, error) {
	// Convert
	value, err := inputDef.Type.ParseValue(value)
	if err != nil {
		return InputValue{}, fmt.Errorf("invalid template input: %w", err)
	}

	// Validate all except oauth inputs
	if isFilled && inputDef.Kind != templateInput.KindOAuth && inputDef.Kind != templateInput.KindOAuthAccounts {
		if err := inputDef.ValidateUserInput(value); err != nil {
			return InputValue{}, fmt.Errorf("invalid template input: %w", err)
		}
	}

	return InputValue{Id: inputDef.Id, Value: value, Skipped: !isFilled}, nil
}

type dependencies interface {
	Logger() log.Logger
	Components() (*model.ComponentsMap, error)
	StorageApiClient() (client.Sender, error)
	SchedulerApiClient() (client.Sender, error)
}

type _reference = model.TemplateRef

type Template struct {
	_reference
	template        repository.TemplateRecord
	version         repository.VersionRecord
	deps            dependencies
	fs              filesystem.Fs
	srcDir          filesystem.Fs
	testsDir        filesystem.Fs
	longDescription string
	readme          string
	manifestFile    *ManifestFile
	inputs          StepsGroups
}

func New(reference model.TemplateRef, template repository.TemplateRecord, version repository.VersionRecord, templateDir, commonDir filesystem.Fs, d dependencies) (*Template, error) {
	// Mount <common> directory to:
	//   template dir FS - used to load manifest, inputs, readme
	//   src dir FS - objects root
	mountPoint := mountfs.NewMountPoint(repository.CommonDirectoryMountPoint, commonDir)
	templateDir, err := aferofs.NewMountFs(templateDir, mountPoint)
	if err != nil {
		return nil, err
	}
	srcDir, err := templateDir.SubDirFs(SrcDirectory)
	if err != nil {
		return nil, err
	}
	srcDir, err = aferofs.NewMountFs(srcDir, mountPoint)
	if err != nil {
		return nil, err
	}

	// Create struct
	out := &Template{_reference: reference, template: template, version: version, deps: d, fs: templateDir, srcDir: srcDir}

	// Load manifest
	out.manifestFile, err = LoadManifest(templateDir)
	if err != nil {
		return nil, err
	}

	// Load inputs
	out.inputs, err = LoadInputs(templateDir)
	if err != nil {
		return nil, err
	}

	// Load long description
	out.longDescription, err = LoadLongDesc(templateDir)
	if err != nil {
		return nil, err
	}

	// Load readme
	out.readme, err = LoadReadme(templateDir)
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

func (t *Template) TestsDir() (filesystem.Fs, error) {
	if t.testsDir == nil {
		if !t.fs.IsDir(TestsDirectory) {
			return nil, fmt.Errorf(`directory "%s" not found`, TestsDirectory)
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

func (t *Template) ListTests() ([]string, error) {
	dirFS, err := t.TestsDir()
	if err != nil {
		return nil, err
	}
	paths, err := dirFS.Glob("*")
	if err != nil {
		return nil, err
	}
	return paths, nil
}

func (t *Template) TestDir(name string) (filesystem.Fs, error) {
	dirFS, err := t.TestsDir()
	if err != nil {
		return nil, err
	}
	if !dirFS.IsDir(name) {
		return nil, fmt.Errorf(`test "%s" not found`, name)
	}

	return dirFS.SubDirFs(name)
}

func (t *Template) TestExpectedOutFS(name string) (filesystem.Fs, error) {
	testFS, err := t.TestDir(name)
	if err != nil {
		return nil, err
	}
	if !testFS.IsDir(ExpectedOutDirectory) {
		return nil, fmt.Errorf(`directory "%s" in test "%s" not found`, ExpectedOutDirectory, name)
	}
	return testFS.SubDirFs(ExpectedOutDirectory)
}

func (t *Template) TestInputs(name string, provider testhelper.EnvProvider, replaceEnvsFn func(string, testhelper.EnvProvider, string) string, envSeparator string) (map[string]interface{}, error) {
	testFS, err := t.TestDir(name)
	if err != nil {
		return nil, err
	}
	return LoadTestInputs(testFS, provider, replaceEnvsFn, envSeparator)
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

func (t *Template) ManifestExists() (bool, error) {
	return t.srcDir.IsFile(t.ManifestPath()), nil
}

func (t *Template) LoadState(ctx Context, options loadState.Options) (*State, error) {
	localFilter := ctx.LocalObjectsFilter()
	remoteFilter := ctx.RemoteObjectsFilter()
	loadOptions := loadState.OptionsWithFilter{
		Options:      options,
		LocalFilter:  &localFilter,
		RemoteFilter: &remoteFilter,
	}

	// Evaluate manifest
	container, err := t.evaluate(ctx)
	if err != nil {
		return nil, err
	}

	// Load state
	if s, err := loadState.Run(container, loadOptions, t.deps); err == nil {
		return NewState(s, container), nil
	} else {
		return nil, err
	}
}

func (t *Template) evaluate(ctx Context) (*evaluatedTemplate, error) {
	// Evaluate manifest
	evaluatedManifest, err := t.manifestFile.Evaluate(ctx.JsonNetContext())
	if err != nil {
		return nil, err
	}

	return &evaluatedTemplate{
		Template: t,
		context:  ctx,
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
	return &model.TemplateMainConfig{ConfigId: mainConfig.Id, ComponentId: mainConfig.ComponentId}, nil
}
