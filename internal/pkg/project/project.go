package project

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/remote"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

type Manifest = projectManifest.Manifest

func LoadManifest(fs filesystem.Fs) (*Manifest, error) {
	return projectManifest.Load(fs)
}

type dependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	StorageApi() (*remote.StorageApi, error)
	SchedulerApi() (*scheduler.Api, error)
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
		fileLoader:   fileloader.New(fs),
		manifest:     manifest,
	}
}

func (p *Project) Fs() filesystem.Fs {
	return p.fs
}

func (p *Project) FileLoader() filesystem.FileLoader {
	return p.fileLoader
}

func (p *Project) Manifest() manifest.Manifest {
	return p.manifest
}

func (p *Project) Filter() model.ObjectsFilter {
	return p.manifest.Filter()
}

func (p *Project) Ctx() context.Context {
	return p.dependencies.Ctx()
}

func (p *Project) MappersFor(state *state.State) mapper.Mappers {
	return MappersFor(state, p.dependencies)
}
