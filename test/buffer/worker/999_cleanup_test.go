package worker

import (
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
)

// test999Cleanup tests the deletion of all objects.
func (ts *testSuite) test999Cleanup() {
	// Delete export1
	n := ts.RandomAPINode()
	assert.NoError(ts.t, n.Service.DeleteExport(n.Dependencies, &buffer.DeleteExportPayload{
		ReceiverID: ts.receiver.ID,
		ExportID:   ts.export1.ID,
	}))

	// Delete receiver (and export2)
	n = ts.RandomAPINode()
	assert.NoError(ts.t, n.Service.DeleteReceiver(n.Dependencies, &buffer.DeleteReceiverPayload{
		ReceiverID: ts.receiver.ID,
	}))

	// Check etcd state
	ts.AssertEtcdState("999-cleanup")

	ts.AssertNoLoggedWarning()
	ts.AssertNoLoggedError()
	ts.TruncateLogs()
}
