package ignore

import (
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *ignoreMapper) AfterRemoteOperation(changes *model.RemoteChanges) error {
	// Ignore objects
	ignored := make(map[string]bool)
	for _, object := range changes.Loaded() {
		if m.isIgnored(object.RemoteState()) {
			ignored[object.Key().String()] = true
			if object.HasLocalState() {
				// Clear remote state
				object.SetRemoteState(nil)
			} else {
				// Remove state
				m.state.Remove(object.Key())
			}
		}
	}

	// Fix list of the changed objects
	changes.Replace(func(v model.ObjectState) model.ObjectState {
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
		if configState, found := m.state.Get(o.ConfigKey()); !found {
			return true
		} else if configState.HasRemoteState() {
			return m.isIgnoredConfig(configState.RemoteState().(*model.Config))
		}
		return false
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
		m.logger.Debugf("Ignored unattached variables %s", config.Desc())
		return true
	}

	return false
}
