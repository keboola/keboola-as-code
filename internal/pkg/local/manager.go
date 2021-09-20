package local

import (
	"go.uber.org/zap"

	"github.com/keboola/keboola-as-code/internal/pkg/manifest"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

type Manager struct {
	logger     *zap.SugaredLogger
	manifest   *manifest.Manifest
	components *model.ComponentsMap
}

func NewManager(logger *zap.SugaredLogger, m *manifest.Manifest, components *model.ComponentsMap) *Manager {
	return &Manager{
		logger:     logger,
		manifest:   m,
		components: components,
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
