package worker

import (
	"time"
)

// test001ImportRecords imports 4 records; 4 < 5 (uploadCountThreshold) - upload is not triggered.
func (ts *testSuite) test001ImportRecords() {
	ts.t.Logf("-------------------------")
	ts.t.Logf("001 import records")
	ts.t.Logf("-------------------------")

	// Run imports immediately after the last check to prevent the check during imports.
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%s][bufferWorker][service][conditions]INFO checked "2" opened slices | %s
	`)

	// Import records 1-4
	for i := 1; i <= 4; i++ {
		ts.Import(i)
	}

	// Check etcd state
	ts.AssertEtcdState("001-import-records")
}
