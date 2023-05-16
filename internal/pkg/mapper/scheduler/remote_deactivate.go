package scheduler

import (
	"github.com/keboola/go-client/pkg/request"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// onRemoteDelete deactivates scheduler by Scheduler API when scheduler configuration is deleted.
func (m *schedulerMapper) onRemoteDelete(grp *request.RunGroup, configState *model.ConfigState) {
	grp.Add(m.KeboolaProjectAPI().DeleteSchedulesForConfigurationRequest(configState.ID))
}
