package transformation

import (
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/local"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
)

type dependencies interface {
	Logger() log.Logger
	Components() (*model.ComponentsMap, error)
}

type transformationLocalMapper struct {
	*helper
	state  *local.State
	logger log.Logger
}

type transformationRemoteMapper struct {
	*helper
	state  *remote.State
	logger log.Logger
}

type helper struct {
	dependencies
}

func NewLocalMapper(s *local.State, d dependencies) *transformationLocalMapper {
	return &transformationLocalMapper{helper: newHelper(d), state: s, logger: d.Logger()}
}

func NewRemoteMapper(s *remote.State, d dependencies) *transformationRemoteMapper {
	return &transformationRemoteMapper{helper: newHelper(d), state: s, logger: d.Logger()}
}

func newHelper(d dependencies) *helper {
	return &helper{dependencies: d}
}

func (h *helper) isTransformation(key model.Key) (bool, error) {
	// Must be config
	configKey, ok := key.(model.ConfigKey)
	if !ok {
		return false, nil
	}
	
	// Get components
	components, err := h.Components()
	if err != nil {
		return false, err
	}

	// Get component
	component, err := components.Get(configKey.ComponentKey())
	if err != nil {
		return false, err
	}

	return component.IsTransformation(), nil
}
