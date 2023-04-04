package key

import (
	"fmt"

	commonKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/store/key"
)

type Key struct {
	ProjectID commonKey.ProjectID `json:"projectId" validate:"required"`
	TaskID    ID                  `json:"taskId" validate:"required"`
}

func (v Key) String() string {
	return fmt.Sprintf("%s/%s", v.ProjectID.String(), v.TaskID.String())
}
