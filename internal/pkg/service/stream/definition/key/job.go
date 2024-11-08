package key

import (
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type JobID string

type JobKey struct {
	SinkKey
	JobID JobID `json:"jobId" validate:"required,min=1,max=48"`
}

func (v JobID) String() string {
	if v == "" {
		panic(errors.New("JobID cannot be empty"))
	}
	return string(v)
}

func (v JobKey) String() string {
	return v.SinkKey.String() + "/" + v.JobID.String()
}

func (v JobKey) Telemetry() []attribute.KeyValue {
	t := v.SinkKey.Telemetry()
	t = append(t, attribute.String("job.id", v.JobID.String()))
	return t
}
