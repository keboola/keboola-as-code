package worker

import (
	"time"
)

// test002SliceUpload imports 2 more records; 6 >= 5 (uploadCountThreshold) - upload is triggered.
func (ts *testSuite) test002SliceUpload() {
	// Run imports immediately after the last check to prevent the check during imports.
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%s][service][conditions]INFO  checked "2" opened slices
	`)

	// Import records
	for i := 5; i <= 6; i++ {
		ts.Import(i)
	}

	// Periodic condition checks have detected that the UPLOAD conditions for both slices/exports have been met.
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%d][service][conditions]INFO  closing slice "%s": count threshold met, received: 6 rows, threshold: 5 rows
[worker-node-%d][service][conditions]INFO  closing slice "%s": count threshold met, received: 6 rows, threshold: 5 rows
`)

	// The slices have transitioned from the "writing" state to the "closing" state by the "slice.swap" task,
	// and new replacement slices in the "writing" state have been created.
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%d][task][%s/slice.swap/%s]INFO  task succeeded (%s): new slice created, the old is closing
[worker-node-%d][task][%s/slice.swap/%s]INFO  task succeeded (%s): new slice created, the old is closing
`)

	// All API nodes have been notified about the changes in the slices states.
	ts.WaitForLogMessages(15*time.Second, `
[api-node-%d][api][watcher]INFO  state updated to the revision "%d"
[api-node-%d][api][watcher]INFO  state updated to the revision "%d"
[api-node-%d][api][watcher]INFO  state updated to the revision "%d"
[api-node-%d][api][watcher]INFO  state updated to the revision "%d"
[api-node-%d][api][watcher]INFO  state updated to the revision "%d"
`)

	// The worker task "slice.close" has been started for both slices that are currently in the "closing" state.
	// These tasks will wait until all API nodes have switched to the new slices.
	// Once all API nodes have confirmed the switch, the tasks will be unblocked,
	// and both slices will transition from the "closing" state to the "uploading" state.
	ts.WaitForLogMessages(15*time.Second, `
[api-node-%d][api][watcher]INFO  reported revision "%d"
[api-node-%d][api][watcher]INFO  reported revision "%d"
[api-node-%d][api][watcher]INFO  reported revision "%d"
[api-node-%d][api][watcher]INFO  reported revision "%d"
[api-node-%d][api][watcher]INFO  reported revision "%d"
[worker-node-%d][task][%s/slice.close/%s]INFO  task succeeded (%s): slice closed
[worker-node-%d][task][%s/slice.close/%s]INFO  task succeeded (%s): slice closed
`)

	// The slices have been uploaded and have now transitioned from the "uploading" state to the "uploaded" state.
	ts.WaitForLogMessages(120*time.Second, `
[worker-node-%d][task][%s/slice.upload/%s]INFO  task succeeded (%s): slice uploaded
[worker-node-%d][task][%s/slice.upload/%s]INFO  task succeeded (%s): slice uploaded
`)
	ts.WaitForLogMessages(5*time.Second, `
[worker-node-%d]DEBUG  Sent "slice-upload" event id: "%d"
[worker-node-%d]DEBUG  Sent "slice-upload" event id: "%d"
`)

	// Check etcd state
	ts.AssertEtcdState("002-slice-upload")

	ts.AssertNoLoggedWarning()
	ts.AssertNoLoggedError()
	ts.TruncateLogs()
}
