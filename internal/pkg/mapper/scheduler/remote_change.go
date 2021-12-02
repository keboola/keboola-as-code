package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *schedulerMapper) OnRemoteChange(changes *model.RemoteChanges) error {
	pool := m.api.NewPool()

	// Activate scheduler on remote save
	for _, objectState := range changes.Saved() {
		if m.isSchedulerConfigFromMainBranch(objectState) {
			m.onRemoteSave(pool, objectState.(*model.ConfigState))
		}
	}

	// Deactivate scheduler on remote delete
	for _, objectState := range changes.Deleted() {
		if m.isSchedulerConfigFromMainBranch(objectState) {
			m.onRemoteDelete(pool, objectState.(*model.ConfigState))
		}
	}

	// Run requests
	return pool.StartAndWait()
}

func (m *schedulerMapper) isSchedulerConfigFromMainBranch(objectState model.ObjectState) bool {
	configState, ok := objectState.(*model.ConfigState)
	if !ok {
		return false
	}

	if configState.ComponentId != model.SchedulerComponentId {
		return false
	}

	branch := m.State.MustGet(configState.BranchKey()).(*model.BranchState)
	return branch.LocalOrRemoteState().(*model.Branch).IsDefault
}
