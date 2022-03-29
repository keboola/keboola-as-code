package project

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/configmetadata"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/corefiles"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/defaultbucket"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/description"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/ignore"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/relations"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/scheduler"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/sharedcode"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/transformation"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper/variables"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
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
		return mapper.Mappers{
			// Core files
			corefiles.NewMapper(s),
			description.NewMapper(),
			// Storage
			defaultbucket.NewMapper(s),
			// Variables
			variables.NewMapper(s),
			sharedcode.NewVariablesMapper(s),
			// Special components
			scheduler.NewMapper(s, d),
			orchestrator.NewMapper(s),
			// Native codes
			transformation.NewMapper(s),
			sharedcode.NewCodesMapper(s),
			// Shared code links
			sharedcode.NewLinksMapper(s),
			// Relations between objects
			relations.NewMapper(s),
			// Skip variables configurations that are not used in any configuration
			ignore.NewMapper(s),
			// Configurations metadata
			configmetadata.NewMapper(s, d),
		}, nil
	}
}
