package worker

import (
	"time"
)

// test002SliceUpload imports 2 more records; 6 >= 5 (uploadCountThreshold) - upload is triggered.
func (ts *testSuite) test002SliceUpload() {
	ts.t.Logf("-------------------------")
	ts.t.Logf("002 slice upload")
	ts.t.Logf("-------------------------")

	// Run imports immediately after the last check to prevent the check during imports.
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%s][bufferWorker][service][conditions]INFO checked "2" opened slices
	`)

	// Import records 5-6
	for i := 5; i <= 6; i++ {
		ts.Import(i)
	}

	// Periodic condition checks have detected that the UPLOAD conditions for both slices/exports have been met.
	ts.t.Logf("waiting for upload conditions check ...")
	if ts.WaitForLogMessages(15*time.Second, `
[worker-node-%d][bufferWorker][task][%s/slice.swap/%s]INFO closing slice "%s": count threshold met, received: 6 rows, threshold: 5 rows
[worker-node-%d][bufferWorker][task][%s/slice.swap/%s]INFO closing slice "%s": count threshold met, received: 6 rows, threshold: 5 rows
`) {
		ts.t.Logf("upload conditions met")
	}

	// The slices have transitioned from the "writing" state to the "closing" state by the "slice.swap" task,
	// and new replacement slices in the "writing" state have been created.
	ts.t.Logf("waiting for slices swap: new slices should be created, old slices should be switched from writing to closing state ...")
	if ts.WaitForLogMessages(15*time.Second, `
[worker-node-%d][bufferWorker][task][%s/slice.swap/%s]INFO task succeeded (%s): new slice created, the old is closing
[worker-node-%d][bufferWorker][task][%s/slice.swap/%s]INFO task succeeded (%s): new slice created, the old is closing
`) {
		ts.t.Logf("slices have been swapped - switched from writing to closing state")
	}

	// All API nodes have been notified about the changes in the slices states.
	ts.t.Logf("waiting for API nodes sync ...")
	if ts.WaitForLogMessages(15*time.Second, `
[api-node-%d][bufferApi][api][watcher]INFO state updated to the revision "%d"
[api-node-%d][bufferApi][api][watcher]INFO state updated to the revision "%d"
[api-node-%d][bufferApi][api][watcher]INFO state updated to the revision "%d"
[api-node-%d][bufferApi][api][watcher]INFO state updated to the revision "%d"
[api-node-%d][bufferApi][api][watcher]INFO state updated to the revision "%d"
`) {
		ts.t.Logf("all API nodes synced to revision with new slices")
	}

	// The worker task "slice.close" has been started for both slices that are currently in the "closing" state.
	// These tasks will wait until all API nodes have switched to the new slices.
	// Once all API nodes have confirmed the switch, the tasks will be unblocked,
	// and both slices will transition from the "closing" state to the "uploading" state.
	ts.t.Logf("waiting for slices to close ...")
	if ts.WaitForLogMessages(15*time.Second, `
[api-node-%d][bufferApi][api][watcher]INFO reported revision "%d"
[api-node-%d][bufferApi][api][watcher]INFO reported revision "%d"
[api-node-%d][bufferApi][api][watcher]INFO reported revision "%d"
[api-node-%d][bufferApi][api][watcher]INFO reported revision "%d"
[api-node-%d][bufferApi][api][watcher]INFO reported revision "%d"
[worker-node-%d][bufferWorker][task][%s/slice.close/%s]INFO task succeeded (%s): slice closed
[worker-node-%d][bufferWorker][task][%s/slice.close/%s]INFO task succeeded (%s): slice closed
`) {
		ts.t.Logf("all API nodes reported the new revision, none writes to old slices anymore")
		ts.t.Logf("slices have been closed - switched from closing to closed state")
	}

	// The slices have been uploaded and have now transitioned from the "uploading" state to the "uploaded" state.
	ts.t.Logf("waiting for slices to upload ...")
	if ts.WaitForLogMessages(120*time.Second, `
[worker-node-%d][bufferWorker][task][%s/slice.upload/%s]INFO task succeeded (%s): slice uploaded
[worker-node-%d][bufferWorker][task][%s/slice.upload/%s]INFO task succeeded (%s): slice uploaded
`) {
		ts.t.Logf("slices have been uploaded")
	}

	// Slices upload events.
	ts.t.Logf("waiting for slices upload events ...")
	ts.WaitForLogMessages(5*time.Second, `
[worker-node-%d][bufferWorker]DEBUG Sent "slice-upload" event id: "%d"
[worker-node-%d][bufferWorker]DEBUG Sent "slice-upload" event id: "%d"
`)
	ts.t.Logf("slices upload events sent")

	// Check etcd state
	ts.AssertEtcdState("002-slice-upload")

	ts.AssertNoLoggedWarning()
	ts.AssertNoLoggedError()
	ts.TruncateLogs()
}
