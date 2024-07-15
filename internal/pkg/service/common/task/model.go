package task

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

type Task struct {
	Key
	Type       string           `json:"type" validate:"required"`
	CreatedAt  utctime.UTCTime  `json:"createdAt" validate:"required"`
	FinishedAt *utctime.UTCTime `json:"finishedAt,omitempty"`
	Node       string           `json:"node" validate:"required"`
	Lock       etcdop.Key       `json:"lock" validate:"required"`
	Result     string           `json:"result,omitempty"`
	Error      string           `json:"error,omitempty"`
	UserError  *Error           `json:"userError,omitempty"`
	Outputs    Outputs          `json:"outputs,omitempty"`
	Duration   *time.Duration   `json:"duration,omitempty"`
}

type Error struct {
	Name        string `json:"name,omitempty"`
	Message     string `json:"message,omitempty"`
	ExceptionID string `json:"exceptionId,omitempty"`
}

type Outputs map[string]any

func (t *Task) IsProcessing() bool {
	return t.FinishedAt == nil
}

func (t *Task) IsSuccessful() bool {
	return !t.IsProcessing() && t.Error == ""
}

func (t *Task) IsFailed() bool {
	return !t.IsProcessing() && t.Error != ""
}
