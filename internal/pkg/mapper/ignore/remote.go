package ignore

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/mapper/orchestrator"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func (m *ignoreMapper) AfterRemoteOperation(ctx context.Context, changes *model.RemoteChanges) error {
	// Ignore objects
	ignored := make(map[string]bool)
	for _, object := range changes.Loaded() {
		if m.isIgnored(ctx, object.RemoteState()) {
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

func (m *ignoreMapper) isIgnored(ctx context.Context, object model.Object) bool {
	switch o := object.(type) {
	case *model.Branch:
		return false
	case *model.Config:
		return m.isIgnoredConfig(ctx, o)
	case *model.ConfigRow:
		// Check parent config
		if configState, found := m.state.Get(o.ConfigKey()); !found {
			return true
		} else if configState.HasRemoteState() {
			return m.isIgnoredConfig(ctx, configState.RemoteState().(*model.Config))
		}
		return false
	default:
		panic(errors.Errorf(`unexpected object type: %T`, object))
	}
}

// isIgnoredConfig ignores all variables configs which are not attached to a config.
func (m *ignoreMapper) isIgnoredConfig(ctx context.Context, config *model.Config) bool {
	// Variables config
	if config.ComponentID == keboola.VariablesComponentID {
		// Without target config
		if !config.Relations.Has(model.VariablesForRelType) && !config.Relations.Has(model.SharedCodeVariablesForRelType) {
			m.logger.Debugf(ctx, "Ignored unattached variables %s", config.Desc())
			return true
		}
		return false
	}

	// Scheduler config
	if config.ComponentID == keboola.SchedulerComponentID {
		relationRaw, err := config.Relations.GetOneByType(model.SchedulerForRelType)
		if err != nil || relationRaw == nil {
			// Relation is missing or invalid, scheduler is ignored
			return true
		}

		// Target config key
		relation := relationRaw.(*model.SchedulerForRelation)
		targetConfigKey := model.ConfigKey{
			BranchID:    config.BranchID,
			ComponentID: relation.ComponentID,
			ID:          relation.ConfigID,
		}

		// Configuration must exists
		if _, found := m.state.RemoteObjects().Get(targetConfigKey); !found {
			m.logger.Debugf(ctx, "Ignored scheduler %s, target %s not found", config.Desc(), targetConfigKey.Desc())
			return true
		}

		// Ignore schedulers targeting orchestrators/flows - they are stored inline in the _config.yml
		// The orchestrator mapper's AfterRemoteOperation runs before this mapper and collects schedule data
		targetComponent, err := m.state.Components().GetOrErr(relation.ComponentID)
		if err == nil && orchestrator.IsOrchestratorOrFlow(targetComponent) {
			m.logger.Debugf(ctx, "Ignored scheduler %s targeting orchestrator/flow %s", config.Desc(), targetConfigKey.Desc())
			return true
		}
	}

	return false
}
