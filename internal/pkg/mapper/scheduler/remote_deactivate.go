package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// onRemoteDelete deactivates scheduler by Scheduler API when scheduler configuration is deleted.
func (m *schedulerMapper) onRemoteDelete(api *schedulerapi.Api, pool *client.Pool, configState *model.ConfigState) {
	pool.Request(api.DeleteSchedulesForConfigurationRequest(configState.Id)).Send()
}
