package job

import (
	keboolaSink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func NewJobKey() keboolaSink.JobKey {
	return keboolaSink.JobKey{
		SinkKey: test.NewSinkKey(),
		JobID:   "1111",
	}
}

func NewTestJob() keboolaSink.Job {
	return keboolaSink.Job{
		JobKey: NewJobKey(),
	}
}

func NewJob(k keboolaSink.JobKey) keboolaSink.Job {
	return keboolaSink.Job{
		JobKey: k,
	}
}
