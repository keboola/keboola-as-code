package remote

import (
	"fmt"
	"time"

	"github.com/go-resty/resty/v2"
	"github.com/spf13/cast"

	"keboola-as-code/src/client"
	"keboola-as-code/src/model"
)

func (a *StorageApi) GetJob(jobId int) (*model.Job, error) {
	response := a.GetJobRequest(jobId).Send().Response
	if response.HasResult() {
		return response.Result().(*model.Job), nil
	}
	return nil, response.Err()
}

// GetJobRequest https://keboola.docs.apiary.io/#reference/jobs/manage-jobs/job-detail
func (a *StorageApi) GetJobRequest(jobId int) *client.Request {
	job := &model.Branch{}
	return a.
		NewRequest(resty.MethodGet, "jobs/{jobId}").
		SetPathParam("jobId", cast.ToString(jobId)).
		SetResult(job)
}

func waitForJob(a *StorageApi, parentRequest *client.Request, job *model.Job, onJobSuccess client.ResponseCallback) client.ResponseCallback {
	// Check job
	backoff := newBackoff()
	var checkJobStatus client.ResponseCallback
	checkJobStatus = func(response *client.Response) {
		// Check status
		if job.Status == "success" {
			if onJobSuccess != nil {
				onJobSuccess(response)
			}
			return
		} else if job.Status == "error" {
			err := fmt.Errorf("job failed: %v", job.Results)
			response.SetErr(err)
			return
		}

		// Wait and check again
		delay := backoff.NextBackOff()
		if delay == backoff.Stop {
			err := fmt.Errorf("timeout: timeout while waiting for the storage job to complete")
			response.SetErr(err)
			return
		}

		// Try again
		request := a.GetJobRequest(job.Id).SetResult(job).OnSuccess(checkJobStatus)
		parentRequest.WaitFor(request)
		time.Sleep(delay)
		response.Sender().Request(request).Send()
	}
	return checkJobStatus
}
