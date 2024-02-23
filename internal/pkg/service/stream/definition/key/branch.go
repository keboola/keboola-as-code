package key

import (
	"fmt"

	"github.com/keboola/go-client/pkg/keboola"
)

type BranchKey struct {
	ProjectID keboola.ProjectID `json:"projectId" validate:"required,min=1"`
	BranchID  keboola.BranchID  `json:"branchId" validate:"required,min=1"`
}

func (v BranchKey) String() string {
	return fmt.Sprintf("%s/%s", v.ProjectID.String(), v.BranchID.String())
}
