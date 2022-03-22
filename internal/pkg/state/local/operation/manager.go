package operation

import (
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/filesystem/knownpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/mapper"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/naming"
	"github.com/keboola/keboola-as-code/internal/pkg/state/local/relatedpaths"
	"github.com/keboola/keboola-as-code/internal/pkg/state/manifest"
)

type Manager struct {
	logger          log.Logger
	fs              filesystem.Fs
	knownPaths      *knownpaths.Paths
	relatedPaths    map[model.Key]*relatedpaths.Paths
	namingRegistry  *naming.Registry
	namingGenerator *naming.Generator
	mapper          *mapper.Mapper
	manifest        manifest.Manifest
}

func NewManager(logger log.Logger, fs filesystem.Fs, mapper *mapper.Mapper, manifest manifest.Manifest) (*Manager, error) {
	knownPaths, err := knownpaths.New(fs)
	if err != nil {
		return nil, err
	}

	return &Manager{
		logger:       logger,
		fs:           fs,
		knownPaths:   knownPaths,
		relatedPaths: make(map[model.Key]*relatedpaths.Paths),
		mapper:       mapper,
		manifest:     manifest,
	}, nil
}

func (m *Manager) pathTo(object model.Object) model.AbsPath {
	// Get
	key := object.Key()
	if path, found := m.namingRegistry.PathByKey(key); found {
		return path
	}

	// Generate
	path := m.namingGenerator.Path(object)
	if err := m.namingRegistry.Attach(key, path); err != nil {
		panic(err)
	}
	return path
}

func (m *Manager) relatedPathsFor(object model.Object) *relatedpaths.Paths {
	key := object.Key()
	if _, found := m.relatedPaths[key]; !found {
		m.relatedPaths[key] = relatedpaths.New(m.pathTo(object).String())
	}
	return m.relatedPaths[key]
}
