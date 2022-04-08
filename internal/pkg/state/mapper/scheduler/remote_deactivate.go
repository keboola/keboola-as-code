package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// onRemoteDelete deactivates scheduler by Scheduler API when scheduler configuration is deleted.
func (m *schedulerRemoteMapper) onRemoteDelete(api *schedulerapi.Api, pool *client.Pool, configKey model.ConfigKey) {
	pool.Request(api.DeleteSchedulesForConfigurationRequest(configKey.ConfigId)).Send()
}
