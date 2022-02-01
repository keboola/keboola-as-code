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
)

type Manifest = projectManifest.Manifest

func LoadManifest(fs filesystem.Fs) (*Manifest, error) {
	return projectManifest.Load(fs)
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*storageapi.Api, error)
	SchedulerApi() (*schedulerapi.Api, error)
}

type Project struct {
	dependencies
	fs         filesystem.Fs
	fileLoader filesystem.FileLoader
	manifest   *Manifest
}

func New(fs filesystem.Fs, manifest *Manifest, d dependencies) *Project {
	return &Project{
		dependencies: d,
		fs:           fs,
		fileLoader:   fs.FileLoader(),
		manifest:     manifest,
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
	return p.dependencies.Ctx()
}

func (p *Project) MappersFor(state *state.State) (mapper.Mappers, error) {
	return MappersFor(state, p.dependencies)
}
