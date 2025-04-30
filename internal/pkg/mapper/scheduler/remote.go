package scheduler

import (
	"context"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/keboola/keboola-sdk-go/v2/pkg/request"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

func (m *schedulerMapper) AfterRemoteOperation(ctx context.Context, changes *model.RemoteChanges) error {
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
		keboolaProjectAPI := m.KeboolaProjectAPI()

		// Create requests pool
		grp := request.NewRunGroup(ctx, keboolaProjectAPI.Client())

		// Activate saved configs
		for _, o := range saved {
			m.onRemoteSave(grp, o)
		}

		// Deactivate deleted configs
		for _, o := range deleted {
			m.onRemoteDelete(grp, o)
		}

		// Run requests
		return grp.RunAndWait()
	}

	return nil
}

func (m *schedulerMapper) isSchedulerConfigFromMainBranch(objectState model.ObjectState) bool {
	configState, ok := objectState.(*model.ConfigState)
	if !ok {
		return false
	}

	if configState.ComponentID != keboola.SchedulerComponentID {
		return false
	}

	branch := m.state.MustGet(configState.BranchKey()).(*model.BranchState)
	return branch.LocalOrRemoteState().(*model.Branch).IsDefault
}
