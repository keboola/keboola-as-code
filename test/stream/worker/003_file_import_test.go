package worker

import (
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"
)

// test003FileImport imports 4 more records; 10 >= 10 (importCountThreshold) - import is triggered.
func (ts *testSuite) test003FileImport() {
	ts.t.Logf("-------------------------")
	ts.t.Logf("003 file import")
	ts.t.Logf("-------------------------")

	// Run imports immediately after the last check to prevent the check during imports.
	ts.WaitForLogMessages(15*time.Second, `
[worker-node-%s][conditions]DEBUG checked "2" opened slices
	`)

	// Send records 7-10
	for i := 7; i <= 10; i++ {
		ts.SendPayload(i)
	}

	// Periodic condition checks have detected that the IMPORT conditions for both files/exports have been met.
	ts.t.Logf("waiting for import conditions check ...")
	if ts.WaitForLogMessages(15*time.Second, `
[worker-node-%d][bufferWorker][conditions]INFO closing file "%s": count threshold met, received: 10 rows, threshold: 10 rows
[worker-node-%d][bufferWorker][conditions]INFO closing file "%s": count threshold met, received: 10 rows, threshold: 10 rows
`) {
		ts.t.Logf("import conditions met")
	}

	// The files and the last slices have now transitioned from the "writing" state to the "closing" state,
	// and new replacement files in the "writing" state have been created.
	ts.t.Logf("waiting for files swap: new files should be created, old files should be switched from writing to closing state ...")
	if ts.WaitForLogMessages(60*time.Second, `
[worker-node-%d][bufferWorker][task][%s/file.swap/%s]INFO task succeeded (%s): new file created, the old is closing
[worker-node-%d][bufferWorker][task][%s/file.swap/%s]INFO task succeeded (%s): new file created, the old is closing
`) {
		ts.t.Logf("files have been swapped - switched from writing to closing state")
	}

	// The slices have been closed.
	ts.t.Logf("waiting for slices to close ...")
	if ts.WaitForLogMessages(15*time.Second, `
[worker-node-%d][bufferWorker][task][%s/slice.close/%s]INFO task succeeded (%s): slice closed
[worker-node-%d][bufferWorker][task][%s/slice.close/%s]INFO task succeeded (%s): slice closed
`) {
		ts.t.Logf("slices have been closed - switched from closing to closed state")
	}

	// The slices have been uploaded.
	ts.t.Logf("waiting for slices to upload ...")
	if ts.WaitForLogMessages(120*time.Second, `
[worker-node-%d][bufferWorker][task][%s/slice.upload/%s]INFO task succeeded (%s): slice uploaded
[worker-node-%d][bufferWorker][task][%s/slice.upload/%s]INFO task succeeded (%s): slice uploaded
`) {
		ts.t.Logf("slices have been uploaded")
	}

	// Slices upload events.
	ts.t.Logf("waiting for slices upload events ...")
	if ts.WaitForLogMessages(5*time.Second, `
[worker-node-%d][bufferWorker]DEBUG Sent "slice-upload" event id: "%d"
[worker-node-%d][bufferWorker]DEBUG Sent "slice-upload" event id: "%d"
`) {
		ts.t.Logf("slices upload events sent")
	}

	// The files have been imported
	ts.t.Logf("waiting for files to close (blocked until all slices were closed) ...")
	ts.t.Logf("all API nodes reported the new revision, old slices were switched from closing to closed state")
	if ts.WaitForLogMessages(15*time.Second, `
[worker-node-%d][bufferWorker][task][%s/file.close/%s]INFO task succeeded (%s): file closed
[worker-node-%d][bufferWorker][task][%s/file.close/%s]INFO task succeeded (%s): file closed
`) {
		ts.t.Logf("files have been closed - switched from closing to closed state")
	}

	// The files have been imported
	ts.t.Logf("waiting for files to import ...")
	if ts.WaitForLogMessages(120*time.Second, `
[worker-node-%d][bufferWorker][task][%s/file.import/%s]INFO task succeeded (%s): file imported
[worker-node-%d][bufferWorker][task][%s/file.import/%s]INFO task succeeded (%s): file imported
`) {
		ts.t.Logf("files have been imported")
	}

	// Files import events.
	ts.t.Logf("waiting for files import events ...")
	if ts.WaitForLogMessages(5*time.Second, `
[worker-node-%d][bufferWorker]DEBUG Sent "file-import" event id: "%d"
[worker-node-%d][bufferWorker]DEBUG Sent "file-import" event id: "%d"
`) {
		ts.t.Logf("files import events sent")
	}

	// Check etcd state
	ts.AssertEtcdState("003-file-import")

	ts.AssertNoLoggedWarning()
	ts.AssertNoLoggedError()
	ts.TruncateLogs()

	// Check tables
	assert.Equal(ts.t, &keboola.TablePreview{
		Columns: []string{"idCol", "bodyCol", "headersCol"},
		Rows: [][]string{
			{"1", `{"key": "payload001"}`, `{"Accept-Encoding":"gzip, br","Content-Type":"application/json","User-Agent":"keboola-go-client"}`},
			{"2", `{"key": "payload002"}`, `{"Accept-Encoding":"gzip, br","Content-Type":"application/json","User-Agent":"keboola-go-client"}`},
			{"3", `{"key": "payload003"}`, `{"Accept-Encoding":"gzip, br","Content-Type":"application/json","User-Agent":"keboola-go-client"}`},
			{"4", `{"key": "payload004"}`, `{"Accept-Encoding":"gzip, br","Content-Type":"application/json","User-Agent":"keboola-go-client"}`},
			{"5", `{"key": "payload005"}`, `{"Accept-Encoding":"gzip, br","Content-Type":"application/json","User-Agent":"keboola-go-client"}`},
			{"6", `{"key": "payload006"}`, `{"Accept-Encoding":"gzip, br","Content-Type":"application/json","User-Agent":"keboola-go-client"}`},
			{"7", `{"key": "payload007"}`, `{"Accept-Encoding":"gzip, br","Content-Type":"application/json","User-Agent":"keboola-go-client"}`},
			{"8", `{"key": "payload008"}`, `{"Accept-Encoding":"gzip, br","Content-Type":"application/json","User-Agent":"keboola-go-client"}`},
			{"9", `{"key": "payload009"}`, `{"Accept-Encoding":"gzip, br","Content-Type":"application/json","User-Agent":"keboola-go-client"}`},
			{"10", `{"key": "payload010"}`, `{"Accept-Encoding":"gzip, br","Content-Type":"application/json","User-Agent":"keboola-go-client"}`},
		},
	}, ts.TablePreview(ts.export1.Mapping.TableID, "bodyCol"))
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
	}, ts.TablePreview(ts.export2.Mapping.TableID, "idCol"))
}
