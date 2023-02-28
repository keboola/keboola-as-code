package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func (c *cluster) CreateExport(t *testing.T, receiver *buffer.Receiver, name string, columns ...*buffer.Column) *buffer.Export {
	t.Helper()

	n := c.RandomAPINode()
	d := n.Dependencies
	svc := n.Service

	// Start a "create export" task
	task, err := svc.CreateExport(d, &buffer.CreateExportPayload{
		ReceiverID: receiver.ID,
		Name:       name,
		Mapping: &buffer.Mapping{
			TableID: "in.c-bucket." + strhelper.NormalizeName(name),
			Columns: columns,
		},
		Conditions: &buffer.Conditions{
			Count: 10,
			Size:  "1MB",
			Time:  "1h",
		},
	})
	assert.NoError(t, err)

	// Wait for the task
	assert.Eventually(t, func() bool {
		task, err = svc.GetTask(d, &buffer.GetTaskPayload{
			ReceiverID: task.ReceiverID,
			Type:       task.Type,
			TaskID:     task.ID,
		})
		assert.NoError(t, err)
		return task.IsFinished
	}, 1*time.Minute, 100*time.Millisecond)
	assert.Nil(t, task.Error)

	// Get export
	export, err := svc.GetExport(d, &buffer.GetExportPayload{
		ReceiverID: receiver.ID,
		ExportID:   buffer.ExportID(strhelper.NormalizeName(name)),
	})
	if err != nil {
		assert.Fail(t, err.Error())
	}

	return export
}
