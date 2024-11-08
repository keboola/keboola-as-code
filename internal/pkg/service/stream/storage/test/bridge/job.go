package job

import (
	"math"
	"strconv"

	"github.com/keboola/go-client/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	keboolaSink "github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/type/tablesink/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func NewTestJob(token string) keboolaSink.Job {
	k := test.NewJobKey()
	storageJobKey, err := strconv.ParseInt(k.JobID.String(), 10, 64)
	if err != nil || storageJobKey > math.MaxInt32 {
		return keboolaSink.Job{}
	}

	return keboolaSink.Job{
		JobKey:        k,
		StorageJobKey: keboola.StorageJobKey{ID: keboola.StorageJobID(storageJobKey)},
		Token:         token,
	}
}

func NewJob(k key.JobKey, token string) keboolaSink.Job {
	storageJobKey, err := strconv.ParseInt(k.JobID.String(), 10, 64)
	if err != nil || storageJobKey > math.MaxInt32 {
		return keboolaSink.Job{}
	}

	return keboolaSink.Job{
		JobKey:        k,
		StorageJobKey: keboola.StorageJobKey{ID: keboola.StorageJobID(storageJobKey)},
		Token:         token,
	}
}
