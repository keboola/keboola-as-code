package job

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	keboolaSink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola/bridge/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func NewJobKey() key.JobKey {
	return key.JobKey{
		SinkKey: test.NewSinkKey(),
		JobID:   "1111",
	}
}

func NewTestJob(token string) keboolaSink.Job {
	return keboolaSink.Job{
		JobKey: NewJobKey(),
		Token:  token,
	}
}

func NewJob(k key.JobKey, token string) keboolaSink.Job {
	return keboolaSink.Job{
		JobKey: k,
		Token:  token,
	}
}
