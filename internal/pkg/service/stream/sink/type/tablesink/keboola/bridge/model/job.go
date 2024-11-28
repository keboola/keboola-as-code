package model

import (
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Job contains all Keboola-specific data we need for polling jobs.
// At the end throttle the sink, so it does not overload the service.
type Job struct {
	JobKey
	Deleted bool `json:"-"` // internal field to mark the entity for deletion, there is no soft delete
}

type JobID string

type JobKey struct {
	key.SinkKey
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
