package scheduler

import (
	"github.com/keboola/keboola-sdk-go/v2/pkg/request"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// onRemoteSave activates scheduler by Scheduler API when scheduler configuration is created/updated.
func (m *schedulerMapper) onRemoteSave(grp *request.RunGroup, configState *model.ConfigState) {
	grp.Add(m.KeboolaProjectAPI().ActivateScheduleRequest(configState.ID, ""))
}
