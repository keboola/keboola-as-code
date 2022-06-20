package ignore

import (
	"context"
	"fmt"

	"github.com/keboola/go-client/pkg/storageapi"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *ignoreMapper) AfterRemoteOperation(_ context.Context, changes *model.RemoteChanges) error {
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
	if config.ComponentId == storageapi.VariablesComponentID {
		// Without target config
		if !config.Relations.Has(model.VariablesForRelType) && !config.Relations.Has(model.SharedCodeVariablesForRelType) {
			m.logger.Debugf("Ignored unattached variables %s", config.Desc())
			return true
		}
		return false
	}

	// Scheduler config
	if config.ComponentId == storageapi.SchedulerComponentID {
		relationRaw, err := config.Relations.GetOneByType(model.SchedulerForRelType)
		if err != nil || relationRaw == nil {
			// Relation is missing or invalid, scheduler is ignored
			return true
		}

		// Target config key
		relation := relationRaw.(*model.SchedulerForRelation)
		targetConfigKey := model.ConfigKey{
			BranchId:    config.BranchId,
			ComponentId: relation.ComponentId,
			Id:          relation.ConfigId,
		}

		// Configuration must exists
		if _, found := m.state.RemoteObjects().Get(targetConfigKey); !found {
			m.logger.Debugf("Ignored scheduler %s, target %s not found", config.Desc(), targetConfigKey.Desc())
			return true
		}
	}

	return false
}
