package worker

import (
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bufferDesign "github.com/keboola/keboola-as-code/api/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
)

// test004EmptyFileAndSlice triggers upload of an empty slice and import of an empty file
// by change in the export mapping.
func (ts *testSuite) test004EmptyFileAndSlice() {
	// Update mapping of the export
	n := ts.RandomAPINode()
	svc, d := n.Service, n.Dependencies
	task, err := svc.UpdateExport(d, &buffer.UpdateExportPayload{
		ReceiverID: ts.receiver.ID,
		ExportID:   ts.export1.ID,
		Mapping: &buffer.Mapping{
			TableID: ts.export1.Mapping.TableID + "-v2",
			Columns: []*buffer.Column{
				{Name: "idCol", Type: "id", PrimaryKey: true},
				{Name: "bodyCol", Type: "body"},
				// Removed headersCol
			},
		},
	})
	assert.NoError(ts.t, err)

	// Wait for the task
	assert.Eventually(ts.t, func() bool {
		task, err = svc.GetTask(d, &buffer.GetTaskPayload{TaskID: task.ID})
		assert.NoError(ts.t, err)
		return task.Status != bufferDesign.TaskStatusProcessing
	}, 1*time.Minute, 100*time.Millisecond)
	assert.Nil(ts.t, task.Error)

	// Get export
	ts.export1, err = svc.GetExport(d, &buffer.GetExportPayload{
		ReceiverID: ts.receiver.ID,
		ExportID:   ts.export1.ID,
	})
	require.NoError(ts.t, err)

	// The slice has been marked as uploaded, the file has been marked as imported.
	ts.WaitForLogMessages(10*time.Second, `
[worker-node-%d][task][%s/slice.close/%s]INFO  task succeeded (%s): slice closed
[worker-node-%d][task][%s/slice.upload/%s]INFO  task succeeded (%s): skipped upload of the empty slice
[worker-node-%d][task][%s/file.close/%s]INFO  task succeeded (%s): file closed
[worker-node-%d][task][%s/file.import/%s]INFO  task succeeded (%s): skipped import of the empty file
`)

	// Check etcd state
	ts.AssertEtcdState("004-empty-file-and-slice")

	ts.AssertNoLoggedWarning()
	ts.AssertNoLoggedError()
	ts.TruncateLogs()
}
