package scheduler

import (
	"github.com/keboola/go-client/pkg/client"

	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// onRemoteSave activates scheduler by Scheduler API when scheduler configuration is created/updated.
func (m *schedulerMapper) onRemoteSave(grp *client.RunGroup, configState *model.ConfigState) {
	grp.Add(m.KeboolaProjectAPI().ActivateScheduleRequest(configState.ID, ""))
}
