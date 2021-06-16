package api

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model/remote"
)

func (a *StorageApi) GetJob(jobId int) (*remote.Job, error) {
	response := a.GetJobRequest(jobId).Send().Response()
	if response.HasResult() {
		return response.Result().(*remote.Job), nil
	}
	return nil, response.Error()
}

// GetJobRequest https://keboola.docs.apiary.io/#reference/jobs/manage-jobs/job-detail
func (a *StorageApi) GetJobRequest(jobId int) *client.Request {
	job := &remote.Branch{}
	return a.
		Request(resty.MethodGet, fmt.Sprintf("jobs/%d", jobId)).
		SetResult(job)
}
