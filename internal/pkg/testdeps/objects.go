package testdeps

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/fileloader"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/state"
)

func NewObjectsContainer(fs filesystem.Fs, m manifest.Manifest) *ObjectsContainer {
	return &ObjectsContainer{
		FsValue:       fs,
		ManifestValue: m,
	}
}

// ObjectsContainer implementation for tests.
type ObjectsContainer struct {
	FsValue       filesystem.Fs
	ManifestValue manifest.Manifest
}

func (p *ObjectsContainer) Ctx() context.Context {
	return context.Background()
}

func (p *ObjectsContainer) Fs() filesystem.Fs {
	return p.FsValue
}

func (p *ObjectsContainer) FileLoader() filesystem.FileLoader {
	return fileloader.New(p.FsValue)
}

func (p *ObjectsContainer) Manifest() manifest.Manifest {
	return p.ManifestValue
}

func (p *ObjectsContainer) MappersFor(_ *state.State) mapper.Mappers {
	return mapper.Mappers{}
}
