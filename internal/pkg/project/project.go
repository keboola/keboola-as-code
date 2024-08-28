package project

import (
	"context"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/env"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	loadState "github.com/keboola/keboola-as-code/pkg/lib/operation/state/load"
)

const (
	BackendSnowflake = "snowflake"
	BackendBigQuery  = "bigquery"
)

type Manifest = projectManifest.Manifest

type InvalidManifestError = projectManifest.InvalidManifestError

func NewManifest(projectID keboola.ProjectID, apiHost string) *Manifest {
	return projectManifest.New(projectID, apiHost)
}

func LoadManifest(ctx context.Context, logger log.Logger, fs filesystem.Fs, envs env.Provider, ignoreErrors bool) (*Manifest, error) {
	return projectManifest.Load(ctx, logger, fs, envs, ignoreErrors)
}

type dependencies interface {
	Components() *model.ComponentsMap
	KeboolaProjectAPI() *keboola.AuthorizedAPI
	Logger() log.Logger
	Telemetry() telemetry.Telemetry
}

type Project struct {
	deps       dependencies
	ctx        context.Context
	fs         filesystem.Fs
	fileLoader filesystem.FileLoader
	manifest   *Manifest
}

func New(ctx context.Context, logger log.Logger, fs filesystem.Fs, envs env.Provider, ignoreErrors bool) (*Project, error) {
	m, err := projectManifest.Load(ctx, logger, fs, envs, ignoreErrors)
	if err != nil {
		return nil, err
	}
	return NewWithManifest(ctx, fs, m), nil
}

func NewWithManifest(ctx context.Context, fs filesystem.Fs, m *Manifest) *Project {
	return &Project{
		ctx:        ctx,
		fs:         fs,
		fileLoader: fs.FileLoader(),
		manifest:   m,
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
	return *p.manifest.Filter()
}

func (p *Project) Ctx() context.Context {
	return p.ctx
}

func (p *Project) MappersFor(state *state.State) (mapper.Mappers, error) {
	return MappersFor(state, p.deps)
}

func (p *Project) LoadState(options loadState.Options, d dependencies) (*State, error) {
	p.deps = d

	// Use filter from the project manifest
	filter := p.Filter()
	loadOptionsWithFilter := loadState.OptionsWithFilter{
		Options:      options,
		LocalFilter:  &filter,
		RemoteFilter: &filter,
	}

	// Load state
	s, err := loadState.Run(p.ctx, p, loadOptionsWithFilter, d)
	if err != nil {
		return nil, err
	}
	return NewState(s, p), nil
}
