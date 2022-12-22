package statistics_test

import (
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"

	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStatsManager(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()
	d := dependencies.NewMockedDeps(t, dependencies.WithClock(clk), dependencies.WithUniqueID("my-node"))
	client := d.EtcdClient()
	node := NewAPINode(d)

	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(clk.Now())}
	sliceKey := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(clk.Now())}
	clk.Add(time.Hour)

	// no notify -> wait 1 second -> no sync
	clk.Add(SyncInterval)
	etcdhelper.AssertKVs(t, client, "")

	// notify -> wait 1 second -> sync
	node.Notify(sliceKey, 1000)
	etcdhelper.ExpectModification(t, client, func() {
		clk.Add(SyncInterval)
	})
	etcdhelper.AssertKVs(t, client, `
<<<<<
stats/received/123/my-receiver/my-export/1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "1970-01-01T00:00:00.000Z",
  "sliceId": "1970-01-01T00:00:00.000Z",
  "count": 1,
  "size": 1000,
  "lastReceivedAt": "1970-01-01T01:00:01.000Z"
}
>>>>>
`)

	// no notify -> wait 1 second -> no sync
	clk.Add(SyncInterval)
	etcdhelper.AssertKVs(t, client, `
<<<<<
stats/received/123/my-receiver/my-export/1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "1970-01-01T00:00:00.000Z",
  "sliceId": "1970-01-01T00:00:00.000Z",
  "count": 1,
  "size": 1000,
  "lastReceivedAt": "1970-01-01T01:00:01.000Z"
}
>>>>>
`)

	// notify -> wait 1 second -> sync
	node.Notify(sliceKey, 2000)
	etcdhelper.ExpectModification(t, client, func() {
		clk.Add(SyncInterval)
	})
	etcdhelper.AssertKVs(t, client, `
<<<<<
stats/received/123/my-receiver/my-export/1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "1970-01-01T00:00:00.000Z",
  "sliceId": "1970-01-01T00:00:00.000Z",
  "count": 2,
  "size": 3000,
  "lastReceivedAt": "1970-01-01T01:00:03.000Z"
}
>>>>>
`)

	// no notify -> wait 1 second -> no sync
	clk.Add(SyncInterval)
	etcdhelper.AssertKVs(t, client, `
<<<<<
stats/received/123/my-receiver/my-export/1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "1970-01-01T00:00:00.000Z",
  "sliceId": "1970-01-01T00:00:00.000Z",
  "count": 2,
  "size": 3000,
  "lastReceivedAt": "1970-01-01T01:00:03.000Z"
}
>>>>>
`)

	// notify before shutdown
	node.Notify(sliceKey, 3000)
	etcdhelper.ExpectModification(t, client, func() {
		d.Process().Shutdown(errors.New("test shutdown"))
		d.Process().WaitForShutdown()
	})

	// shutdown triggered sync
	etcdhelper.AssertKVs(t, client, `
<<<<<
stats/received/123/my-receiver/my-export/1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "1970-01-01T00:00:00.000Z",
  "sliceId": "1970-01-01T00:00:00.000Z",
  "count": 3,
  "size": 6000,
  "lastReceivedAt": "1970-01-01T01:00:05.000Z"
}
>>>>>
`)

	// check logs
	expected := `
INFO  process unique id "%s"
[stats]DEBUG  syncing 1 records
[stats]DEBUG  sync done
[stats]DEBUG  syncing 1 records
[stats]DEBUG  sync done
INFO  exiting (test shutdown)
[stats]INFO  received shutdown request
[stats]DEBUG  syncing 1 records
[stats]DEBUG  sync done
[stats]INFO  shutdown done
INFO  exited
`
	wildcards.Assert(t, strings.TrimSpace(expected), d.DebugLogger().AllMessages())
}
