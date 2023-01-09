package model

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

type Task struct {
	key.TaskKey
	FinishedAt *key.UTCTime   `json:"finishedAt,omitempty"`
	WorkerNode string         `json:"workerNode" validate:"required"`
	Lock       string         `json:"lock" validate:"required"`
	Result     string         `json:"result,omitempty"`
	Error      string         `json:"error,omitempty"`
	Duration   *time.Duration `json:"duration,omitempty"`
}
