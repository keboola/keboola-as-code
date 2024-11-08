package test

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

func NewJobKey() key.JobKey {
	return key.JobKey{
		SinkKey: NewSinkKey(),
		JobID:   "1111",
	}
}

func NewJob(k key.JobKey) model.Job {
	return model.Job{JobKey: k}
}
