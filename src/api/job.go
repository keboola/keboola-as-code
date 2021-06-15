package api

import (
	"fmt"
	"github.com/go-resty/resty/v2"
	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
	"time"
)

func (a *StorageApi) GetJob(jobId int) (*model.Job, error) {
	response := a.GetJobRequest(jobId).Send().Response()
	if response.HasResult() {
		return response.Result().(*model.Job), nil
	}
	return nil, response.Error()
}

// GetJobRequest https://keboola.docs.apiary.io/#reference/jobs/manage-jobs/job-detail
func (a *StorageApi) GetJobRequest(jobId int) *client.Request {
	job := &model.Branch{}
	return a.
		NewRequest(resty.MethodGet, fmt.Sprintf("jobs/%d", jobId)).
		SetResult(job)
}

func waitForSuccessJob(a *StorageApi, job *model.Job, onJobSuccess client.ResponseCallback) client.ResponseCallback {
	// Check job
	backoff := a.createBackoff()
	var checkJob client.ResponseCallback
	checkJob = func(response *client.Response) *client.Response {
		if job.Status == "success" {
			if onJobSuccess != nil {
				onJobSuccess(response)
			}
			return response
		} else if job.Status == "error" {
			return response.SetError(fmt.Errorf("create branch job failed: %v", job.Results))
		}

		// Wait and check again
		delay := backoff.NextBackOff()
		if delay == backoff.Stop {
			return response.SetError(fmt.Errorf("timeout: checking waiting storage job - create branch"))
		}
		time.Sleep(delay)

		// Try again
		request := a.GetJobRequest(job.Id).SetResult(job).OnSuccess(checkJob)
		response.Sender().Send(request)
		return response
	}
	return checkJob
}
