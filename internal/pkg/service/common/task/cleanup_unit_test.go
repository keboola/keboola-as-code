package task

import (
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
)

func TestIsForCleanup(t *testing.T) {
	t.Parallel()

	node := &Cleaner{clock: clockwork.NewRealClock()}

	// Unfinished task, too recent
	createdAt := utctime.UTCTime(time.Now().Add(-1 * time.Hour))
	task := Task{
		Key:        Key{},
		CreatedAt:  createdAt,
		FinishedAt: nil,
		Error:      "",
	}
	assert.False(t, node.isForCleanup(task))

	// Unfinished task, too old
	createdAt = utctime.UTCTime(time.Now().Add(-30 * 24 * time.Hour))
	task = Task{
		Key:        Key{},
		CreatedAt:  createdAt,
		FinishedAt: nil,
		Error:      "",
	}
	assert.True(t, node.isForCleanup(task))

	// Finished task, successful, too recent
	createdAt = utctime.UTCTime(time.Now().Add(-1 * time.Minute))
	task = Task{
		Key:        Key{},
		CreatedAt:  createdAt,
		FinishedAt: &createdAt,
		Error:      "",
	}
	assert.False(t, node.isForCleanup(task))

	// Finished task, successful, too old
	createdAt = utctime.UTCTime(time.Now().Add(-2 * time.Hour))
	task = Task{
		Key:        Key{},
		CreatedAt:  createdAt,
		FinishedAt: &createdAt,
		Error:      "",
	}
	assert.True(t, node.isForCleanup(task))

	// Finished task, error, too recent
	createdAt = utctime.UTCTime(time.Now().Add(-2 * time.Hour))
	task = Task{
		Key:        Key{},
		CreatedAt:  createdAt,
		FinishedAt: &createdAt,
		Error:      "error",
	}
	assert.False(t, node.isForCleanup(task))

	// Finished task, successful, too old
	createdAt = utctime.UTCTime(time.Now().Add(-48 * time.Hour))
	task = Task{
		Key:        Key{},
		CreatedAt:  createdAt,
		FinishedAt: &createdAt,
		Error:      "error",
	}
	assert.True(t, node.isForCleanup(task))
}
