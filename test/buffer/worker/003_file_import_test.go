package worker

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
)

// test003FileImport imports 4 more records; 10 >= 10 (importCountThreshold) - import is triggered.
func (ts *testSuite) test003FileImport() {
	// Run imports immediately after the last check to prevent the check during imports.
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%s][service][conditions]INFO  checked "2" opened slices
	`)

	// Import records
	for i := 7; i <= 10; i++ {
		ts.Import(i)
	}

	// Periodic condition checks have detected that the IMPORT conditions for both files/exports have been met.
	// The files and the last slices have now transitioned from the "writing" state to the "closing" state,
	// and new replacement files in the "writing" state have been created.
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%d][service][conditions]INFO  closing file "%s": count threshold met, received: 10 rows, threshold: 10 rows
[worker-node-%d][service][conditions]INFO  closing file "%s": count threshold met, received: 10 rows, threshold: 10 rows
`)
	ts.WaitForLogMessages(60*time.Second, `
[worker-node-%d][task][%s/file.swap/%s]INFO  task succeeded (%s): new file created, the old is closing
[worker-node-%d][task][%s/file.swap/%s]INFO  task succeeded (%s): new file created, the old is closing
`)

	// The slices have been closed and uploaded.
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%d][task][%s/slice.close/%s]INFO  task succeeded (%s): slice closed
[worker-node-%d][task][%s/slice.close/%s]INFO  task succeeded (%s): slice closed
`)
	ts.WaitForLogMessages(120*time.Second, `
[worker-node-%d][task][%s/slice.upload/%s]INFO  task succeeded (%s): slice uploaded
[worker-node-%d][task][%s/slice.upload/%s]INFO  task succeeded (%s): slice uploaded
`)
	ts.WaitForLogMessages(5*time.Second, `
[worker-node-%d]DEBUG  Sent "slice-upload" event id: "%d"
[worker-node-%d]DEBUG  Sent "slice-upload" event id: "%d"
`)

	// The files have been imported
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%d][task][%s/file.close/%s]INFO  task succeeded (%s): file closed
[worker-node-%d][task][%s/file.close/%s]INFO  task succeeded (%s): file closed
`)
	ts.WaitForLogMessages(120*time.Second, `
[worker-node-%d][task][%s/file.import/%s]INFO  task succeeded (%s): file imported
[worker-node-%d][task][%s/file.import/%s]INFO  task succeeded (%s): file imported
`)
	ts.WaitForLogMessages(5*time.Second, `
[worker-node-%d]DEBUG  Sent "file-import" event id: "%d"
[worker-node-%d]DEBUG  Sent "file-import" event id: "%d"
`)

	// Check etcd state
	ts.AssertEtcdState("003-file-import")

	ts.AssertNoLoggedWarning()
	ts.AssertNoLoggedError()
	ts.TruncateLogs()

	// Check tables
	assert.Equal(ts.t, &keboola.TablePreview{
		Columns: []string{"idCol", "bodyCol", "headersCol"},
		Rows: [][]string{
			{"1", `{"key": "payload001"}`, `{"Content-Type":"application/json"}`},
			{"2", `{"key": "payload002"}`, `{"Content-Type":"application/json"}`},
			{"3", `{"key": "payload003"}`, `{"Content-Type":"application/json"}`},
			{"4", `{"key": "payload004"}`, `{"Content-Type":"application/json"}`},
			{"5", `{"key": "payload005"}`, `{"Content-Type":"application/json"}`},
			{"6", `{"key": "payload006"}`, `{"Content-Type":"application/json"}`},
			{"7", `{"key": "payload007"}`, `{"Content-Type":"application/json"}`},
			{"8", `{"key": "payload008"}`, `{"Content-Type":"application/json"}`},
			{"9", `{"key": "payload009"}`, `{"Content-Type":"application/json"}`},
			{"10", `{"key": "payload010"}`, `{"Content-Type":"application/json"}`},
		},
	}, ts.tablePreview(ts.export1.Mapping.TableID, "bodyCol"))
	assert.Equal(ts.t, &keboola.TablePreview{
		Columns: []string{"idCol", "keyValueCol"},
		Rows: [][]string{
			{"<date>", `"---payload001---"`},
			{"<date>", `"---payload002---"`},
			{"<date>", `"---payload003---"`},
			{"<date>", `"---payload004---"`},
			{"<date>", `"---payload005---"`},
			{"<date>", `"---payload006---"`},
			{"<date>", `"---payload007---"`},
			{"<date>", `"---payload008---"`},
			{"<date>", `"---payload009---"`},
			{"<date>", `"---payload010---"`},
		},
	}, ts.tablePreview(ts.export2.Mapping.TableID, "idCol"))
}
