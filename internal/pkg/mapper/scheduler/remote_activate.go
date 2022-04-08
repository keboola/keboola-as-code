package scheduler

import (
	"github.com/keboola/keboola-as-code/internal/pkg/api/client/schedulerapi"
	"github.com/keboola/keboola-as-code/internal/pkg/http/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// onRemoteSave activates scheduler by Scheduler API when scheduler configuration is created/updated.
func (m *schedulerMapper) onRemoteSave(api *schedulerapi.Api, pool *client.Pool, configState *model.ConfigState) {
	pool.Request(api.ActivateScheduleRequest(configState.Id, "")).Send()
}
