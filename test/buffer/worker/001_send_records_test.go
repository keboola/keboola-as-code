package worker

import (
	"time"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

// test001SendPayload sends 4 records; 4 < 5 (uploadCountThreshold) - upload is not triggered.
func (ts *testSuite) test001SendPayload() {
	ts.t.Logf("-------------------------")
	ts.t.Logf("001 send records")
	ts.t.Logf("-------------------------")

	// Run imports immediately after the last check to prevent the check during imports.
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%s][bufferWorker][service][conditions]DEBUG checked "2" opened slices | %s
	`)

	// Send records 1-4
	for i := 1; i <= 4; i++ {
		ts.SendPayload(i)
	}

	// Check etcd state
	// Statistics are ignored, because they are tracked per API node and for each request is used a random API node.
	ts.AssertEtcdState("001-send-payload", etcdhelper.WithIgnoredKeyPattern(`^stats/`))
}
