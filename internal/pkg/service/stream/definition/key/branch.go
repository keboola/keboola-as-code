package key

import (
	"strconv"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"go.opentelemetry.io/otel/attribute"
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

func (v BranchKey) Telemetry() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("project.id", v.ProjectID.String()),
		attribute.String("branch.id", v.BranchID.String()),
	}
}

func (v BranchIDOrDefault) Default() bool {
	return v == "default"
}

func (v BranchIDOrDefault) Int() (int, error) {
	return strconv.Atoi(string(v))
}
