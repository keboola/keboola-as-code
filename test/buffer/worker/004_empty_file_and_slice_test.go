package worker

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/http/buffer/server"
)

// test004EmptyFileAndSlice triggers upload of an empty slice and import of an empty file
// by change in the export mapping.
func (ts *testSuite) test004EmptyFileAndSlice() {
	ts.t.Logf("-------------------------")
	ts.t.Logf("004 empty file and slice")
	ts.t.Logf("-------------------------")

	newTableID := ts.export1.Mapping.TableID + "-v2"
	ts.export1 = ts.UpdateExport(ts.export1.ReceiverID, ts.export1.ID, &server.MappingRequestBody{
		TableID: &newTableID,
		Columns: columnsModeToBody([]*buffer.Column{
			{Name: "idCol", Type: "id", PrimaryKey: true},
			{Name: "bodyCol", Type: "body"},
			// Removed headersCol
		}),
	})

	// The slice has been marked as uploaded, the file has been marked as imported.
	ts.t.Logf("waiting for slice to close, slice is empty, upload and import should be skipped ...")
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%d][bufferWorker][task][%s/slice.close/%s]INFO task succeeded (%s): slice closed
[worker-node-%d][bufferWorker][task][%s/slice.upload/%s]INFO task succeeded (%s): skipped upload of the empty slice
[worker-node-%d][bufferWorker][task][%s/file.close/%s]INFO task succeeded (%s): file closed
[worker-node-%d][bufferWorker][task][%s/file.import/%s]INFO task succeeded (%s): skipped import of the empty file
`)
	ts.t.Logf("slice has been closed - upload and import have been skipped")

	// Check etcd state
	ts.AssertEtcdState("004-empty-file-and-slice")

	ts.AssertNoLoggedWarning()
	ts.AssertNoLoggedError()
	ts.TruncateLogs()
}
