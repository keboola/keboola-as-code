package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
)

func (c *cluster) CreateReceiver(t *testing.T, name string) *buffer.Receiver {
	t.Helper()

	n := c.RandomAPINode()
	d := n.Dependencies
	svc := n.Service

	// Start a "create receiver" task
	task, err := svc.CreateReceiver(d, &buffer.CreateReceiverPayload{Name: name})
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
	}, 10*time.Second, 100*time.Millisecond)
	assert.Nil(t, task.Error)

	// Get receiver
	receiver, err := svc.GetReceiver(d, &buffer.GetReceiverPayload{
		ReceiverID: "my-receiver",
	})
	if err != nil {
		assert.Fail(t, err.Error())
	}

	return receiver
}
