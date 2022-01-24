package testdeps

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

// ObjectsContainer implementation for tests.
type ObjectsContainer struct {
	FsValue        filesystem.Fs
	ManifestValue  manifest.Manifest
	VariablesValue *fileloader.Variables
}

func NewObjectsContainer(fs filesystem.Fs, m manifest.Manifest, variables *fileloader.Variables) *ObjectsContainer {
	return &ObjectsContainer{
		FsValue:        fs,
		ManifestValue:  m,
		VariablesValue: variables,
	}
}

func (c *ObjectsContainer) Ctx() context.Context {
	return context.Background()
}

func (c *ObjectsContainer) Fs() filesystem.Fs {
	return c.FsValue
}

func (c *ObjectsContainer) Variables() *fileloader.Variables {
	return c.VariablesValue
}

func (c *ObjectsContainer) Manifest() manifest.Manifest {
	return c.ManifestValue
}

func (c *ObjectsContainer) MappersFor(_ *state.State) mapper.Mappers {
	return mapper.Mappers{}
}
