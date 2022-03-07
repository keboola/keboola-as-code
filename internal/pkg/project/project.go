package project

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/api/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/api/storageapi"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

type Manifest = projectManifest.Manifest

type InvalidManifestError = projectManifest.InvalidManifestError

func NewManifest(projectId int, apiHost string) *Manifest {
	return projectManifest.New(projectId, apiHost)
}

func LoadManifest(fs filesystem.Fs, ignoreErrors bool) (*Manifest, error) {
	return projectManifest.Load(fs, ignoreErrors)
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
	SchedulerApi() (*schedulerapi.Api, error)
}

type Project struct {
	deps       dependencies
	fs         filesystem.Fs
	fileLoader filesystem.FileLoader
	manifest   *Manifest
}

func New(fs filesystem.Fs, manifest *Manifest, d dependencies) *Project {
	return &Project{
		deps:       d,
		fs:         fs,
		fileLoader: fs.FileLoader(),
		manifest:   manifest,
	}
}

func (p *Project) Fs() filesystem.Fs {
	return p.fs
}

func (p *Project) ObjectsRoot() filesystem.Fs {
	return p.fs
}

func (p *Project) Manifest() manifest.Manifest {
	return p.manifest
}

func (p *Project) ProjectManifest() *Manifest {
	return p.manifest
}

func (p *Project) Filter() model.ObjectsFilter {
	return p.manifest.Filter()
}

func (p *Project) Ctx() context.Context {
	return p.deps.Ctx()
}

func (p *Project) MappersFor(state *state.State) (mapper.Mappers, error) {
	return MappersFor(state, p.deps)
}

func (p *Project) LoadState(options loadState.Options) (*State, error) {
	// Use filter from the project manifest
	filter := p.Filter()
	loadOptionsWithFilter := loadState.OptionsWithFilter{
		Options:      options,
		LocalFilter:  &filter,
		RemoteFilter: &filter,
	}

	// Load state
	s, err := loadState.Run(p, loadOptionsWithFilter, p.deps)
	if err != nil {
		return nil, err
	}
	return NewState(s, p), nil
}
