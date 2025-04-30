package task

import (
	"fmt"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
)

type Key struct {
	// SystemTask means an internal task that is not scoped to a project, for example "tasks.cleanup"
	SystemTask bool              `json:"systemTask,omitempty"`
	ProjectID  keboola.ProjectID `json:"projectId,omitempty" validate:"required_without=SystemTask"`
	TaskID     ID                `json:"taskId" validate:"required"`
}

func (v Key) String() string {
	if v.SystemTask {
		return fmt.Sprintf("_system_/%s", v.TaskID.String())
	}
	return fmt.Sprintf("%s/%s", v.ProjectID.String(), v.TaskID.String())
}

// IsSystemTask returns true if the task is an internal task and it is not scoped to a project, for example "tasks.cleanup".
func (v Key) IsSystemTask() bool {
	return v.SystemTask
}
