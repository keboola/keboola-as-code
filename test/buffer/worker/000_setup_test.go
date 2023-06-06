package worker

import (
	"strings"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
)

// test000Setup creates receiver with two exports.
func (ts *testSuite) test000Setup() {
	ts.t.Logf("-------------------------")
	ts.t.Logf("000 setup")
	ts.t.Logf("-------------------------")

	ts.receiver = ts.CreateReceiver("My Receiver")
	ts.secret = ts.receiver.URL[strings.LastIndex(ts.receiver.URL, "/")+1:]
	ts.export1 = ts.CreateExport(ts.receiver, "My Export 1",
		columnsModeToBody([]*buffer.Column{
			{Name: "idCol", Type: "id", PrimaryKey: true},
			{Name: "bodyCol", Type: "body"},
			{Name: "headersCol", Type: "headers"},
		}),
	)
	ts.export2 = ts.CreateExport(ts.receiver, "My Export 2",
		columnsModeToBody([]*buffer.Column{
			{Name: "idCol", Type: "datetime", PrimaryKey: true},
			{Name: "keyValueCol", Type: "template", Template: &buffer.Template{Language: "jsonnet", Content: `"---" + Body("key") + "---"`}},
		}),
	)

	ts.AssertEtcdState("000-setup")
	ts.AssertNoLoggedWarning()
	ts.AssertNoLoggedError()
	ts.TruncateLogs()
}
