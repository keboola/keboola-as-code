package local

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/filesystem"
	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Manager struct {
	logger   *zap.SugaredLogger
	fs       filesystem.Fs
	manifest *manifest.Manifest
	state    *model.State
}

func NewManager(logger *zap.SugaredLogger, fs filesystem.Fs, m *manifest.Manifest, state *model.State) *Manager {
	return &Manager{
		logger:   logger,
		fs:       fs,
		manifest: m,
		state:    state,
	}
}

func (m *Manager) ProjectDir() string {
	return m.manifest.ProjectDir
}

func (m *Manager) Manifest() *manifest.Manifest {
	return m.manifest
}

func (m *Manager) Naming() model.Naming {
	return m.manifest.Naming
}

func (m *Manager) isTransformationConfig(object interface{}) (bool, error) {
	if v, ok := object.(*model.Config); ok {
		if component, err := m.components.Get(*v.ComponentKey()); err == nil {
			return component.IsTransformation(), nil
		} else {
			return false, err
		}
	}
	return false, nil
}
