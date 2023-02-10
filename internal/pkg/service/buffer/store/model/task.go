package model

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type Task struct {
	key.TaskKey
	CreatedAt  UTCTime        `json:"createdAt" validate:"required"`
	FinishedAt *key.UTCTime   `json:"finishedAt,omitempty"`
	WorkerNode string         `json:"workerNode" validate:"required"`
	Lock       string         `json:"lock" validate:"required"`
	Result     string         `json:"result,omitempty"`
	Error      string         `json:"error,omitempty"`
	Duration   *time.Duration `json:"duration,omitempty"`
}

func (t *Task) IsFinished() bool {
	return t.FinishedAt != nil
}

func (t *Task) IsSuccessful() bool {
	return t.Error == ""
}

func (t *Task) IsForCleanup() bool {
	now := time.Now()
	if !t.IsFinished() {
		taskAge := now.Sub(t.CreatedAt.Time())
		if taskAge < 14*24*time.Hour {
			return false
		}
		// Delete unfinished tasks older than 14 days.
	} else {
		taskAge := now.Sub(t.FinishedAt.Time())
		if t.IsSuccessful() {
			if taskAge < 1*time.Hour {
				return false
			}
			// Delete finished tasks older than 1 hour.
		} else {
			if taskAge < 24*time.Hour {
				return false
			}
			// Delete failed tasks older than 24 hours.
		}
	}
	return true
}
