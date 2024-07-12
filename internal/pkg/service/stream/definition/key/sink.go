package key

import (
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type SinkID string

type SinkKey struct {
	SourceKey
	SinkID SinkID `json:"sinkId" validate:"required,min=1,max=48"`
}

func (v SinkID) String() string {
	if v == "" {
		panic(errors.New("SinkID cannot be empty"))
	}
	return string(v)
}

func (v SinkKey) String() string {
	return v.SourceKey.String() + "/" + v.SinkID.String()
}
