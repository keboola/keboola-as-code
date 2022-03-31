package template

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	templateInput "github.com/keboola/keboola-as-code/internal/pkg/template/input"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

const (
	IdRegexp       = `^[a-zA-Z0-9\-]+$`
	SrcDirectory   = `src`
	TestsDirectory = `tests`
)

type (
	Manifest     = templateManifest.Manifest
	ManifestFile = templateManifest.File
	Input        = templateInput.Input
	Inputs       = templateInput.Inputs
	InputValue   = templateInput.Value
	InputsValues = templateInput.Values
	StepsGroups  = templateInput.StepsGroups
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

type dependencies interface {
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
	SchedulerApi() (*schedulerapi.Api, error)
}

type _reference = model.TemplateRef

type Template struct {
	_reference
	deps         dependencies
	fs           filesystem.Fs
	srcDir       filesystem.Fs
	testsDir     filesystem.Fs
	readme       string
	manifestFile *ManifestFile
	inputs       StepsGroups
}

func New(reference model.TemplateRef, fs filesystem.Fs, manifestFile *ManifestFile, inputs StepsGroups, d dependencies) (*Template, error) {
	// Src dir
	srcDir, err := fs.SubDirFs(SrcDirectory)
	if err != nil {
		return nil, err
	}

	return &Template{_reference: reference, deps: d, fs: fs, srcDir: srcDir, manifestFile: manifestFile, inputs: inputs}, nil
}

func (t *Template) Reference() model.TemplateRef {
	return t._reference
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

func (t *Template) Readme() string {
	return t.readme
}

func (t *Template) Inputs() StepsGroups {
	return t.inputs
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
	m, err := t.manifestFile.Evaluate(ctx.JsonNetContext())
	if err != nil {
		return nil, err
	}

	return &evaluatedTemplate{
		Template: t,
		context:  ctx,
		manifest: m,
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
