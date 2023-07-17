package collector_test

import (
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"

	apiConfig "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	dependencies "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/strhelper"
)

func TestStatsManager(t *testing.T) {
	t.Parallel()

	syncInterval := time.Second
	clk := clock.NewMock()
	apiScp, mock := bufferDependencies.NewMockedAPIScope(
		t,
		apiConfig.NewAPIConfig().Apply(apiConfig.WithStatisticsSyncInterval(syncInterval)),
		dependencies.WithEnabledEtcdClient(),
		dependencies.WithClock(clk),
		dependencies.WithUniqueID("my-node"),
	)
	client := mock.TestEtcdClient()
	node := apiScp.StatsCollector()

	receiverKey := key.ReceiverKey{ProjectID: 123, ReceiverID: "my-receiver"}
	exportKey := key.ExportKey{ExportID: "my-export", ReceiverKey: receiverKey}
	fileKey := key.FileKey{ExportKey: exportKey, FileID: key.FileID(clk.Now())}
	sliceKey := key.SliceKey{FileKey: fileKey, SliceID: key.SliceID(clk.Now())}
	clk.Add(time.Hour)

	// no notify -> wait 1 second -> no sync
	clk.Add(syncInterval)
	etcdhelper.AssertKVsString(t, client, "", etcdhelper.WithIgnoredKeyPattern("^runtime"))

	// notify -> wait 1 second -> sync
	node.Notify(sliceKey, 1000, 1100)
	etcdhelper.ExpectModification(t, client, func() {
		clk.Add(syncInterval)
	})
	etcdhelper.AssertKVsString(t, client, `
<<<<<
stats/received/123/my-receiver/my-export/1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "1970-01-01T00:00:00.000Z",
  "sliceId": "1970-01-01T00:00:00.000Z",
  "nodeId": "my-node",
  "lastRecordAt": "1970-01-01T01:00:01.000Z",
  "recordsCount": 1,
  "recordsSize": "1000B",
  "bodySize": "1100B"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^runtime"))

	// no notify -> wait 1 second -> no sync
	clk.Add(syncInterval)
	etcdhelper.AssertKVsString(t, client, `
<<<<<
stats/received/123/my-receiver/my-export/1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "1970-01-01T00:00:00.000Z",
  "sliceId": "1970-01-01T00:00:00.000Z",
  "nodeId": "my-node",
  "lastRecordAt": "1970-01-01T01:00:01.000Z",
  "recordsCount": 1,
  "recordsSize": "1000B",
  "bodySize": "1100B"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^runtime"))

	// notify -> wait 1 second -> sync
	node.Notify(sliceKey, 2000, 2200)
	etcdhelper.ExpectModification(t, client, func() {
		clk.Add(syncInterval)
	})
	etcdhelper.AssertKVsString(t, client, `
<<<<<
stats/received/123/my-receiver/my-export/1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "1970-01-01T00:00:00.000Z",
  "sliceId": "1970-01-01T00:00:00.000Z",
  "nodeId": "my-node",
  "lastRecordAt": "1970-01-01T01:00:03.000Z",
  "recordsCount": 2,
  "recordsSize": "3000B",
  "bodySize": "3300B"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^runtime"))

	// no notify -> wait 1 second -> no sync
	clk.Add(syncInterval)
	etcdhelper.AssertKVsString(t, client, `
<<<<<
stats/received/123/my-receiver/my-export/1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "1970-01-01T00:00:00.000Z",
  "sliceId": "1970-01-01T00:00:00.000Z",
  "nodeId": "my-node",
  "lastRecordAt": "1970-01-01T01:00:03.000Z",
  "recordsCount": 2,
  "recordsSize": "3000B",
  "bodySize": "3300B"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^runtime"))

	// notify before shutdown
	node.Notify(sliceKey, 3000, 3300)
	etcdhelper.ExpectModification(t, client, func() {
		apiScp.Process().Shutdown(errors.New("test shutdown"))
		apiScp.Process().WaitForShutdown()
	})

	// shutdown triggered sync
	etcdhelper.AssertKVsString(t, client, `
<<<<<
stats/received/123/my-receiver/my-export/1970-01-01T00:00:00.000Z/1970-01-01T00:00:00.000Z/my-node
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "1970-01-01T00:00:00.000Z",
  "sliceId": "1970-01-01T00:00:00.000Z",
  "nodeId": "my-node",
  "lastRecordAt": "1970-01-01T01:00:05.000Z",
  "recordsCount": 3,
  "recordsSize": "6000B",
  "bodySize": "6600B"
}
>>>>>
`, etcdhelper.WithIgnoredKeyPattern("^runtime"))

	// check logs
	expected := `
[stats]DEBUG  syncing 1 records
[stats]DEBUG  sync done
[stats]DEBUG  syncing 1 records
[stats]DEBUG  sync done
[stats]INFO  received shutdown request
[stats]DEBUG  syncing 1 records
[stats]DEBUG  sync done
[stats]INFO  shutdown done
`
	wildcards.Assert(t, strings.TrimSpace(expected), strhelper.FilterLines(`^\[stats\]`, mock.DebugLogger().AllMessages()))
}
