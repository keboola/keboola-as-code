package worker

import (
	"time"
)

// test001ImportRecords imports 4 records; 4 < 5 (uploadCountThreshold) - upload is not triggered.
func (ts *testSuite) test001ImportRecords() {
	// Run imports immediately after the last check to prevent the check during imports.
	ts.WaitForLogMessages(10*time.Second, `
[worker-node-%s][service][conditions]INFO  checked "2" opened slices
	`)

	// Import records
	for i := 1; i <= 4; i++ {
		ts.Import(i)
	}

	// Check etcd state
	ts.AssertEtcdState("001-import-records")
}
