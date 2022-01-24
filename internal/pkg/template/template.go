package template

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/template/replacekeys"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	templateInput "github.com/keboola/keboola-as-code/internal/pkg/template/input"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const IdRegexp = `^[a-zA-Z0-9\-]+$`

type (
	Manifest = templateManifest.Manifest
	Inputs   = templateInput.Inputs
)

func LoadManifest(fs filesystem.Fs) (*Manifest, error) {
	return templateManifest.Load(fs)
}

func NewInputs() *Inputs {
	return templateInput.NewInputs()
}

func LoadInputs(fs filesystem.Fs) (*Inputs, error) {
	return templateInput.Load(fs)
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*remote.StorageApi, error)
	SchedulerApi() (*scheduler.Api, error)
}

type Template struct {
	dependencies
	fs           filesystem.Fs
	fileLoader   filesystem.FileLoader
	manifest     *Manifest
	inputs       *Inputs
	replacements replacekeys.Keys
}

func New(fs filesystem.Fs, manifest *Manifest, inputs *Inputs, replacements replacekeys.Keys, d dependencies) *Template {
	return &Template{
		dependencies: d,
		fs:           fs,
		fileLoader:   fs.FileLoader(),
		manifest:     manifest,
		inputs:       inputs,
		replacements: replacements,
	}
}

func (t *Template) Fs() filesystem.Fs {
	return t.fs
}

func (t *Template) Variables() *fileloader.Variables {
	return fileloader.NewVariables()
}

func (t *Template) Manifest() manifest.Manifest {
	return t.manifest
}

func (t *Template) Inputs() *Inputs {
	return t.inputs
}

func (t *Template) Ctx() context.Context {
	return context.WithValue(t.dependencies.Ctx(), validator.DisableRequiredInProjectKey, true)
}

func (t *Template) MappersFor(state *state.State) mapper.Mappers {
	return MappersFor(state, t.dependencies, t.replacements)
}
