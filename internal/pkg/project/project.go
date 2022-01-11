package project

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
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
	Logger() log.Logger
	StorageApi() (*remote.StorageApi, error)
	SchedulerApi() (*scheduler.Api, error)
}

type Project struct {
	dependencies
	fs       filesystem.Fs
	manifest *Manifest
}

func New(fs filesystem.Fs, manifest *Manifest, d dependencies) *Project {
	return &Project{
		dependencies: d,
		fs:           fs,
		manifest:     manifest,
	}
}

func (p *Project) Fs() filesystem.Fs {
	return p.fs
}

func (p *Project) Manifest() manifest.Manifest {
	return p.manifest
}

func (p *Project) MappersFor(state *state.State) mapper.Mappers {
	return MappersFor(state, p.dependencies)
}
