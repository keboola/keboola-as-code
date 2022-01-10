package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *schedulerMapper) OnRemoteChange(changes *model.RemoteChanges) error {
	var saved []*model.ConfigState
	var deleted []*model.ConfigState

	// Activate scheduler on remote save
	for _, objectState := range changes.Saved() {
		if m.isSchedulerConfigFromMainBranch(objectState) {
			saved = append(saved, objectState.(*model.ConfigState))
		}
	}

	// Deactivate scheduler on remote delete
	for _, objectState := range changes.Deleted() {
		if m.isSchedulerConfigFromMainBranch(objectState) {
			deleted = append(deleted, objectState.(*model.ConfigState))
		}
	}

	if len(saved) > 0 || len(deleted) > 0 {
		// Get Scheduler API - only if it is needed
		api, err := m.SchedulerApi()
		if err != nil {
			return err
		}

		// Create requests pool
		pool := api.NewPool()

		// Activate saved configs
		for _, o := range saved {
			m.onRemoteSave(api, pool, o)
		}

		// Deactivate deleted configs
		for _, o := range deleted {
			m.onRemoteDelete(api, pool, o)
		}

		// Run requests
		return pool.StartAndWait()
	}

	return nil
}

func (m *schedulerMapper) isSchedulerConfigFromMainBranch(objectState model.ObjectState) bool {
	configState, ok := objectState.(*model.ConfigState)
	if !ok {
		return false
	}

	if configState.ComponentId != model.SchedulerComponentId {
		return false
	}

	branch := m.state.MustGet(configState.BranchKey()).(*model.BranchState)
	return branch.LocalOrRemoteState().(*model.Branch).IsDefault
}
