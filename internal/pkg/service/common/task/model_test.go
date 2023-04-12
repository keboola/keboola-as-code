package task

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	taskKey "github.com/keboola/keboola-as-code/internal/pkg/service/common/task/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func TestTask_ForCleanup(t *testing.T) {
	t.Parallel()

	// Unfinished task, too recent
	createdAt := utctime.UTCTime(time.Now().Add(-1 * time.Hour))
	task := &Model{
		Key:        taskKey.Key{},
		CreatedAt:  createdAt,
		FinishedAt: nil,
		Error:      "",
	}
	assert.False(t, task.IsForCleanup())

	// Unfinished task, too old
	createdAt = utctime.UTCTime(time.Now().Add(-30 * 24 * time.Hour))
	task = &Model{
		Key:        taskKey.Key{},
		CreatedAt:  createdAt,
		FinishedAt: nil,
		Error:      "",
	}
	assert.True(t, task.IsForCleanup())

	// Finished task, successful, too recent
	createdAt = utctime.UTCTime(time.Now().Add(-1 * time.Minute))
	task = &Model{
		Key:        taskKey.Key{},
		CreatedAt:  createdAt,
		FinishedAt: &createdAt,
		Error:      "",
	}
	assert.False(t, task.IsForCleanup())

	// Finished task, successful, too old
	createdAt = utctime.UTCTime(time.Now().Add(-2 * time.Hour))
	task = &Model{
		Key:        taskKey.Key{},
		CreatedAt:  createdAt,
		FinishedAt: &createdAt,
		Error:      "",
	}
	assert.True(t, task.IsForCleanup())

	// Finished task, error, too recent
	createdAt = utctime.UTCTime(time.Now().Add(-2 * time.Hour))
	task = &Model{
		Key:        taskKey.Key{},
		CreatedAt:  createdAt,
		FinishedAt: &createdAt,
		Error:      "error",
	}
	assert.False(t, task.IsForCleanup())

	// Finished task, successful, too old
	createdAt = utctime.UTCTime(time.Now().Add(-48 * time.Hour))
	task = &Model{
		Key:        taskKey.Key{},
		CreatedAt:  createdAt,
		FinishedAt: &createdAt,
		Error:      "error",
	}
	assert.True(t, task.IsForCleanup())
}
