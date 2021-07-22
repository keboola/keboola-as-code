package local

import (
	"go.uber.org/zap"
	"keboola-as-code/src/manifest"
	"keboola-as-code/src/model"
	"keboola-as-code/src/remote"
)

type Manager struct {
	logger   *zap.SugaredLogger
	manifest *manifest.Manifest
	api      *remote.StorageApi
}

func NewManager(logger *zap.SugaredLogger, m *manifest.Manifest, api *remote.StorageApi) *Manager {
	return &Manager{
		logger:   logger,
		manifest: m,
		api:      api,
	}
}

func (m *Manager) ProjectDir() string {
	return m.manifest.ProjectDir
}

func (m *Manager) Naming() *model.Naming {
	return m.manifest.Naming
}

func (m *Manager) isTransformationConfig(object interface{}) (bool, error) {
	if v, ok := object.(*model.Config); ok {
		if component, err := m.api.Components().Get(*v.ComponentKey()); err == nil {
			return component.IsTransformation(), nil
		} else {
			return false, err
		}
	}
	return false, nil
}
