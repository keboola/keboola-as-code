package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
	"github.com/keboola/keboola-as-code/internal/pkg/scheduler"
)

// onRemoteDelete deactivates scheduler by Scheduler API when scheduler configuration is deleted.
func (m *schedulerMapper) onRemoteDelete(api *scheduler.Api, pool *client.Pool, configState *model.ConfigState) {
	pool.Request(api.DeleteSchedulesForConfigurationRequest(configState.Id)).Send()
}
