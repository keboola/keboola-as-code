package scheduler

import (
	"github.com/keboola/go-client/pkg/client"
	"github.com/keboola/go-client/pkg/schedulerapi"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// onRemoteDelete deactivates scheduler by Scheduler API when scheduler configuration is deleted.
func (m *schedulerMapper) onRemoteDelete(grp *client.RunGroup, configState *model.ConfigState) {
	grp.Add(schedulerapi.DeleteSchedulesForConfigurationRequest(configState.ID))
}
