package task

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"
)

type Key struct {
	ProjectID keboola.ProjectID `json:"projectId" validate:"required"`
	TaskID    ID                `json:"taskId" validate:"required"`
}

func (v Key) String() string {
	return fmt.Sprintf("%s/%s", v.ProjectID.String(), v.TaskID.String())
}
