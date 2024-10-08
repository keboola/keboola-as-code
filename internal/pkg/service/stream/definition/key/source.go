package key

import (
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type SourceID string

type SourceKey struct {
	BranchKey
	SourceID SourceID `json:"sourceId" validate:"required,min=1,max=48"`
}

func (v SourceID) String() string {
	if v == "" {
		panic(errors.New("SourceID cannot be empty"))
	}
	return string(v)
}

func (v SourceKey) String() string {
	return v.BranchKey.String() + "/" + v.SourceID.String()
}

func (v SourceKey) Telemetry() []attribute.KeyValue {
	t := v.BranchKey.Telemetry()
	t = append(t, attribute.String("source.id", v.SourceID.String()))
	return t
}
