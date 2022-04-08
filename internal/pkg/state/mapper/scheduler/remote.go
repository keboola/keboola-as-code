package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/state/backend/remote"
)

type remoteDependencies interface {
	Components() (*model.ComponentsMap, error)
	SchedulerApi() (*schedulerapi.Api, error)
}

type schedulerRemoteMapper struct {
	remoteDependencies
	state *remote.State
}

func NewRemoteMapper(s *remote.State, d remoteDependencies) *schedulerRemoteMapper {
	return &schedulerRemoteMapper{state: s, remoteDependencies: d}
}

func (m *schedulerRemoteMapper) AfterRemoteOperation(changes *model.Changes) error {
	var saved []*model.Config
	var deleted []model.ConfigKey

	// Activate scheduler on remote save
	for _, object := range changes.Saved() {
		if m.isSchedulerFromMainBranch(object.Key()) {
			saved = append(saved, object.(*model.Config))
		}
	}

	// Deactivate scheduler on remote delete
	for _, key := range changes.Deleted() {
		if m.isSchedulerFromMainBranch(key) {
			deleted = append(deleted, key.(model.ConfigKey))
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
		for _, object := range saved {
			m.onRemoteSave(api, pool, object)
		}

		// Deactivate deleted configs
		for _, key := range deleted {
			m.onRemoteDelete(api, pool, key)
		}

		// Run requests
		return pool.StartAndWait()
	}

	return nil
}

func (m *schedulerRemoteMapper) isSchedulerFromMainBranch(key model.Key) bool {
	configKey, ok := key.(model.ConfigKey)
	if !ok {
		return false
	}

	if configKey.ComponentId != model.SchedulerComponentId {
		return false
	}

	branch := m.state.MustGet(configKey.BranchKey()).(*model.Branch)
	return branch.IsDefault
}
