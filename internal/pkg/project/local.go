package project

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/mapper"
)

type Manifest = projectManifest.Manifest

type Project = projectManifest.Project

type InvalidManifestError = projectManifest.InvalidManifestError

func ManifestPath() string {
	return projectManifest.Path()
}

func LoadProjectFromManifest(fs filesystem.Fs, manifestPath string) (Project, error) {
	return projectManifest.ProjectInfo(fs, manifestPath)
}

func NewManifest(ctx context.Context, fs filesystem.Fs, projectId int, apiHost string) *Manifest {
	return projectManifest.New(ctx, fs, projectId, apiHost)
}

func LoadManifest(ctx context.Context, fs filesystem.Fs, ignoreErrors bool) (*Manifest, error) {
	return projectManifest.Load(ctx, fs, ignoreErrors)
}

type localDependencies interface {
	Ctx() context.Context
	Logger() log.Logger
	Components() (*model.ComponentsMap, error)
}

func LocalMappers(d localDependencies) local.MappersFactory {
	return func(s *local.State) (mapper.Mappers, error) {
		return mapper.Mappers{}, nil
	}
}
