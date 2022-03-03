package template

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	templateInput "github.com/keboola/keboola-as-code/internal/pkg/template/input"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
)

const (
	IdRegexp       = `^[a-zA-Z0-9\-]+$`
	SrcDirectory   = `src`
	TestsDirectory = `tests`
)

type (
	Manifest     = templateManifest.Manifest
	Input        = templateInput.Input
	Inputs       = templateInput.Inputs
	InputValue   = templateInput.Value
	InputsValues = templateInput.Values
)

func LoadManifest(fs filesystem.Fs, jsonNetCtx *jsonnet.Context) (*Manifest, error) {
	return templateManifest.Load(fs, jsonNetCtx)
}

func NewInputs() *Inputs {
	return templateInput.NewInputs()
}

func LoadInputs(fs filesystem.Fs) (*Inputs, error) {
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
	fs       filesystem.Fs
	srcDir   filesystem.Fs
	testsDir filesystem.Fs
	readme   string
	inputs   *Inputs
}

func New(reference model.TemplateRef, fs filesystem.Fs, inputs *Inputs) (*Template, error) {
	// Src dir
	srcDir, err := fs.SubDirFs(SrcDirectory)
	if err != nil {
		return nil, err
	}

	return &Template{_reference: reference, fs: fs, srcDir: srcDir, inputs: inputs}, nil
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

func (t *Template) Inputs() *Inputs {
	return t.inputs
}

func (t *Template) ManifestPath() string {
	return templateManifest.Path()
}

func (t *Template) ManifestExists() (bool, error) {
	return t.srcDir.IsFile(t.ManifestPath()), nil
}

func (t *Template) ToObjectsContainer(ctx Context, m *Manifest, d dependencies) *ObjectsContainer {
	return &ObjectsContainer{
		Template:     t,
		dependencies: d,
		context:      ctx,
		manifest:     m,
	}
}

type ObjectsContainer struct {
	*Template
	dependencies
	context  Context
	manifest *Manifest
}

func (c *ObjectsContainer) Manifest() manifest.Manifest {
	return c.manifest
}

func (c *ObjectsContainer) TemplateManifest() *Manifest {
	return c.manifest
}

func (c *ObjectsContainer) TemplateCtx() Context {
	return c.context
}

func (c *ObjectsContainer) Ctx() context.Context {
	return c.context
}

func (c *ObjectsContainer) MappersFor(state *state.State) (mapper.Mappers, error) {
	return MappersFor(state, c.dependencies, c.context)
}
