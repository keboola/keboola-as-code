package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// onRemoteSave activates scheduler by Scheduler API when scheduler configuration is created/updated.
func (m *schedulerRemoteMapper) onRemoteSave(api *schedulerapi.Api, pool *client.Pool, config *model.Config) {
	pool.Request(api.ActivateScheduleRequest(config.ConfigId, "")).Send()
}
