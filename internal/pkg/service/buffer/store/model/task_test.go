package model

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
)

func TestTask_ForCleanup(t *testing.T) {
	t.Parallel()

	// Unfinished task, too recent
	age := key.UTCTime(time.Now().Add(-1 * time.Hour))
	task := &Task{
		TaskKey:    key.TaskKey{},
		CreatedAt:  age,
		FinishedAt: nil,
		Error:      "",
	}
	assert.False(t, task.IsForCleanup())

	// Unfinished task, too old
	age = key.UTCTime(time.Now().Add(-30 * 24 * time.Hour))
	task = &Task{
		TaskKey:    key.TaskKey{},
		CreatedAt:  age,
		FinishedAt: nil,
		Error:      "",
	}
	assert.True(t, task.IsForCleanup())

	// Finished task, successful, too recent
	age = key.UTCTime(time.Now().Add(-1 * time.Minute))
	task = &Task{
		TaskKey:    key.TaskKey{},
		CreatedAt:  age,
		FinishedAt: &age,
		Error:      "",
	}
	assert.False(t, task.IsForCleanup())

	// Finished task, successful, too old
	age = key.UTCTime(time.Now().Add(-2 * time.Hour))
	task = &Task{
		TaskKey:    key.TaskKey{},
		CreatedAt:  age,
		FinishedAt: &age,
		Error:      "",
	}
	assert.True(t, task.IsForCleanup())

	// Finished task, error, too recent
	age = key.UTCTime(time.Now().Add(-2 * time.Hour))
	task = &Task{
		TaskKey:    key.TaskKey{},
		CreatedAt:  age,
		FinishedAt: &age,
		Error:      "error",
	}
	assert.False(t, task.IsForCleanup())

	// Finished task, successful, too old
	age = key.UTCTime(time.Now().Add(-48 * time.Hour))
	task = &Task{
		TaskKey:    key.TaskKey{},
		CreatedAt:  age,
		FinishedAt: &age,
		Error:      "error",
	}
	assert.True(t, task.IsForCleanup())
}
