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
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacekeys"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	templateInput "github.com/keboola/keboola-as-code/internal/pkg/template/input"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	IdRegexp       = `^[a-zA-Z0-9\-]+$`
	SrcDirectory   = `src`
	TestsDirectory = `tests`
)

type (
	Manifest = templateManifest.Manifest
	Inputs   = templateInput.Inputs
)

func LoadManifest(fs filesystem.Fs, jsonNetCtx *jsonnet.Context) (*Manifest, error) {
	return templateManifest.Load(fs, jsonNetCtx)
}

func NewInputs(inputs []templateInput.Input) *Inputs {
	return templateInput.NewInputs(inputs)
}

func LoadInputs(fs filesystem.Fs) (*Inputs, error) {
	return templateInput.Load(fs)
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
	SchedulerApi() (*schedulerapi.Api, error)
}

type Template struct {
	fs       filesystem.Fs
	srcDir   filesystem.Fs
	testsDir filesystem.Fs
	readme   string
	inputs   *Inputs
}

func New(fs filesystem.Fs, inputs *Inputs) (*Template, error) {
	// Src dir
	srcDir, err := fs.SubDirFs(SrcDirectory)
	if err != nil {
		return nil, err
	}

	return &Template{fs: fs, srcDir: srcDir, inputs: inputs}, nil
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

func (t *Template) ToEvaluated(m *Manifest, jsonNetCtx *jsonnet.Context, replacements replacekeys.Keys, d dependencies) *EvaluatedTemplate {
	return &EvaluatedTemplate{
		Template:     t,
		dependencies: d,
		manifest:     m,
		jsonNetCtx:   jsonNetCtx,
		replacements: replacements,
	}
}

type EvaluatedTemplate struct {
	*Template
	dependencies
	manifest     *Manifest
	jsonNetCtx   *jsonnet.Context
	replacements replacekeys.Keys
}

func (t *EvaluatedTemplate) Manifest() manifest.Manifest {
	return t.manifest
}

func (t *EvaluatedTemplate) TemplateManifest() *Manifest {
	return t.manifest
}

func (t *EvaluatedTemplate) Ctx() context.Context {
	return context.WithValue(t.dependencies.Ctx(), validator.DisableRequiredInProjectKey, true)
}

func (t *EvaluatedTemplate) MappersFor(state *state.State) mapper.Mappers {
	return MappersFor(state, t.dependencies, t.jsonNetCtx, t.replacements)
}
