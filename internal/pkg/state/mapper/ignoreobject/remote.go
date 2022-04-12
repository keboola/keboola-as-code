package ignoreobject

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
)

type ignoreMapper struct {
	state  *remote.State
	logger log.Logger
}

type dependencies interface {
	Logger() log.Logger
}

func NewRemoteMapper(s *remote.State, d dependencies) *ignoreMapper {
	return &ignoreMapper{state: s, logger: d.Logger()}
}

func (m *ignoreMapper) AfterRemoteOperation(_ *remote.State, changes *model.Changes) error {
	// Ignore objects
	ignored := make(map[string]bool)
	for _, object := range changes.Loaded() {
		if m.isIgnored(object) {
			ignored[object.Key().String()] = true
			m.state.Remove(object.Key())
		}
	}

	// Fix list of the changed objects
	changes.Replace(func(v model.Object) model.Object {
		if ignored[v.Key().String()] {
			// Remove
			return nil
		}
		// No change
		return v
	})

	return nil
}

func (m *ignoreMapper) isIgnored(object model.Object) bool {
	switch o := object.(type) {
	case *model.Branch:
		return false
	case *model.Config:
		return m.isIgnoredConfig(o)
	case *model.ConfigRow:
		// Check parent config
		if config, found := m.state.Get(o.ConfigKey); !found {
			return true
		} else {
			return m.isIgnoredConfig(config.(*model.Config))
		}
	default:
		panic(fmt.Errorf(`unexpected object type: %T`, object))
	}
}

// isIgnoredConfig ignores all variables configs which are not attached to a config.
func (m *ignoreMapper) isIgnoredConfig(config *model.Config) bool {
	// Variables config
	if config.ComponentId != model.VariablesComponentId {
		return false
	}

	// Without target config
	if !config.Relations.Has(model.VariablesForRelType) && !config.Relations.Has(model.SharedCodeVariablesForRelType) {
		m.logger.Debugf("Ignored unattached variables %s", config.String())
		return true
	}

	return false
}
