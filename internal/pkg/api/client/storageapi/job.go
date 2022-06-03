package storageapi

import (
	"fmt"
	"net/http"
	"time"

	"github.com/spf13/cast"

	. "github.com/keboola/keboola-as-code/internal/pkg/api/client"
	"github.com/keboola/keboola-as-code/internal/pkg/model"
)

// GetJobRequest https://keboola.docs.apiary.io/#reference/jobs/manage-jobs/job-detail
func GetJobRequest(jobId int) Request[*model.Job] {
	return getJobRequest(&model.Job{Id: jobId})
}

func getJobRequest(job *model.Job) Request[*model.Job] {
	return newRequest(job).
		SetMethod(http.MethodGet).
		SetUrl("jobs/{jobId}").
		SetPathParam("jobId", cast.ToString(job.Id))
}

func waitForJob(sender Sender, job *model.Job) error {
	backoff := newBackoff()
	for {
		// Get job status
		if _, _, err := getJobRequest(job).Send(sender); err != nil {
			return err
		}

		// Check status
		if job.Status == "success" {
			return nil
		} else if job.Status == "error" {
			return fmt.Errorf("job failed: %v", job.Results)
		}

		// Wait and check again
		if delay := backoff.NextBackOff(); delay == backoff.Stop {
			return fmt.Errorf("timeout while waiting for the storage job %d to complete", job.Id)
		} else {
			time.Sleep(delay)
		}
	}
}
