package template

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	templateInput "github.com/keboola/keboola-as-code/internal/pkg/template/input"
	templateManifest "github.com/keboola/keboola-as-code/internal/pkg/template/manifest"
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
	Logger() log.Logger
	StorageApi() (*remote.StorageApi, error)
	SchedulerApi() (*scheduler.Api, error)
}

type Template struct {
	dependencies
	fs       filesystem.Fs
	manifest *Manifest
	inputs   *Inputs
}

func New(fs filesystem.Fs, manifest *Manifest, inputs *Inputs, d dependencies) *Template {
	return &Template{
		dependencies: d,
		fs:           fs,
		manifest:     manifest,
		inputs:       inputs,
	}
}

func (p *Template) Fs() filesystem.Fs {
	return p.fs
}

func (p *Template) Manifest() manifest.Manifest {
	return p.manifest
}

func (p *Template) Inputs() *Inputs {
	return p.inputs
}

func (p *Template) MappersFor(state *state.State) mapper.Mappers {
	return MappersFor(state, p.dependencies)
}
