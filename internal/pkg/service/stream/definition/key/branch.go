package key

import (
	"strconv"

	"github.com/keboola/go-client/pkg/keboola"
)

// BranchIDOrDefault is used in the API payloads, it should contain "default" string, or branchID (int).
type BranchIDOrDefault string

type BranchKey struct {
	ProjectID keboola.ProjectID `json:"projectId" validate:"required,min=1"`
	BranchID  keboola.BranchID  `json:"branchId" validate:"required,min=1"`
}

func (v BranchKey) String() string {
	return strconv.Itoa(int(v.ProjectID)) + "/" + v.BranchID.String()
}

func (v BranchIDOrDefault) Default() bool {
	return v == "default"
}

func (v BranchIDOrDefault) Int() (int, error) {
	return strconv.Atoi(string(v))
}
