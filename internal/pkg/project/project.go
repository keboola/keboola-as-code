package project

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	projectManifest "github.com/keboola/keboola-as-code/internal/pkg/project/manifest"
)

type Manifest = projectManifest.Manifest

func LoadManifest(fs filesystem.Fs) (*Manifest, error) {
	return projectManifest.Load(fs)
}

type Project struct {
	fs       filesystem.Fs
	manifest *Manifest
}

func New(fs filesystem.Fs, manifest *Manifest) *Project {
	return &Project{
		fs:       fs,
		manifest: manifest,
	}
}

func (p *Project) Fs() filesystem.Fs {
	return p.fs
}

func (p *Project) Manifest() manifest.Manifest {
	return p.manifest
}
