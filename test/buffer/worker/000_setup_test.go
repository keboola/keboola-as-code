package worker

import (
	"strings"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/gen/buffer"
)

// test000Setup creates receiver with two exports.
func (ts *testSuite) test000Setup() {
	ts.receiver = ts.CreateReceiver(ts.t, "my-receiver")
	assert.NotEmpty(ts.t, ts.receiver)

	ts.secret = ts.receiver.URL[strings.LastIndex(ts.receiver.URL, "/")+1:]
	assert.NotEmpty(ts.t, ts.secret)

	ts.export1 = ts.CreateExport(ts.t, ts.receiver, "my-export-1",
		&buffer.Column{Name: "idCol", Type: "id", PrimaryKey: true},
		&buffer.Column{Name: "bodyCol", Type: "body"},
		&buffer.Column{Name: "headersCol", Type: "headers"},
	)
	assert.NotEmpty(ts.t, ts.export1)

	ts.export2 = ts.CreateExport(ts.t, ts.receiver, "my-export-2",
		&buffer.Column{Name: "idCol", Type: "datetime", PrimaryKey: true},
		&buffer.Column{Name: "keyValueCol", Type: "template", Template: &buffer.Template{Language: "jsonnet", Content: `"---" + Body("key") + "---"`}},
	)
	assert.NotEmpty(ts.t, ts.export2)

	ts.AssertEtcdState("000-setup")
	ts.AssertNoLoggedWarning()
	ts.AssertNoLoggedError()
	ts.AssertLoggedLines(`
[api-node-%d][task][%s/receiver.create/%s]INFO  started task
[api-node-%d][task][%s/receiver.create/%s]DEBUG  lock acquired "runtime/lock/task/%s/my-receiver/receiver.create"
[api-node-%d][task][%s/receiver.create/%s]INFO  task succeeded (%s): receiver created outputs: {"receiverId":"my-receiver"}
[api-node-%d][task][%s/receiver.create/%s]DEBUG  lock released "runtime/lock/task/%s/my-receiver/receiver.create"
[api-node-%d][task][%s/export.create/%s]INFO  started task
[api-node-%d][task][%s/export.create/%s]DEBUG  lock acquired "runtime/lock/task/%s/my-receiver/my-export-1/export.create"
[api-node-%d][task][%s/export.create/%s]INFO  task succeeded (%s): export created outputs: {"exportId":"my-export-1","receiverId":"my-receiver"}
[api-node-%d][task][%s/export.create/%s]DEBUG  lock released "runtime/lock/task/%s/my-receiver/my-export-1/export.create"
[api-node-%d][task][%s/export.create/%s]INFO  started task
[api-node-%d][task][%s/export.create/%s]DEBUG  lock acquired "runtime/lock/task/%s/my-receiver/my-export-2/export.create"
[api-node-%d][task][%s/export.create/%s]INFO  task succeeded (%s): export created outputs: {"exportId":"my-export-2","receiverId":"my-receiver"}
[api-node-%d][task][%s/export.create/%s]DEBUG  lock released "runtime/lock/task/%s/my-receiver/my-export-2/export.create"
`)
	ts.TruncateLogs()
}
