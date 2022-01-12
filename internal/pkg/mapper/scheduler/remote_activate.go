package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
)

// onRemoteSave activates scheduler by Scheduler API when scheduler configuration is created/updated.
func (m *schedulerMapper) onRemoteSave(api *scheduler.Api, pool *client.Pool, configState *model.ConfigState) {
	pool.Request(api.ActivateScheduleRequest(configState.Id, "")).Send()
}
