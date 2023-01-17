package upload_test

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/api/receive"
	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/statistics"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/store/slicestate"
	workerservice "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/service"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestSliceUploadTask(t *testing.T) {
	t.Parallel()

	// Test dependencies
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	clk := clock.NewMock()
	clk.Set(time.Time{}.Add(time.Second))
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)
	opts := []dependencies.MockedOption{dependencies.WithClock(clk), dependencies.WithEtcdNamespace(etcdNamespace)}

	// Simulate API node
	apiDeps1 := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("my-api-1"))...)
	apiDeps2 := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("my-api-2"))...)
	str := apiDeps1.Store()
	sliceKey := createExport(t, ctx, clk, client, str)
	receiverKey := sliceKey.ReceiverKey

	// Start worker node
	workerDeps := bufferDependencies.NewMockedDeps(t, append(opts, dependencies.WithUniqueID("my-worker"))...)
	workerDeps.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	_, err := workerservice.New(workerDeps)
	assert.NoError(t, err)

	// Create 3+5 records
	createRecords(t, ctx, clk, apiDeps1, receiverKey, 1, 3)
	createRecords(t, ctx, clk, apiDeps2, receiverKey, 4, 8)
	clk.Add(statistics.SyncInterval) // sync statistics
	assertRecords(t, client)

	// Switch slice to the closing state
	clk.Add(time.Minute)
	slice, err := str.GetSlice(ctx, sliceKey)
	assert.NoError(t, err)
	_ = etcdhelper.ExpectModification(t, client, func() {
		ok, err := str.SetSliceState(ctx, &slice, slicestate.Closing)
		assert.True(t, ok)
		assert.NoError(t, err)
	})

	// Shutdown
	time.Sleep(time.Second)
	apiDeps1.Process().Shutdown(errors.New("bye bye API 1"))
	apiDeps1.Process().WaitForShutdown()
	apiDeps2.Process().Shutdown(errors.New("bye bye API 2"))
	apiDeps2.Process().WaitForShutdown()
	workerDeps.Process().Shutdown(errors.New("bye bye Worker"))
	workerDeps.Process().WaitForShutdown()

	// Check logs
	wildcards.Assert(t, "", workerDeps.DebugLogger().AllMessages())
}

func createRecords(t *testing.T, ctx context.Context, clk *clock.Mock, d bufferDependencies.Mocked, key key.ReceiverKey, start, count int) {
	t.Helper()

	// Import 5 records
	importer := receive.NewImporter(d)
	d.RequestHeaderMutable().Set("Content-Type", "application/json")
	for i := start; i <= count; i++ {
		clk.Add(time.Second)
		body := io.NopCloser(strings.NewReader(fmt.Sprintf(`{"key":"value%03d"}`, i)))
		assert.NoError(t, importer.CreateRecord(ctx, d, key, receiverSecret, body))
	}
}

func assertRecords(t *testing.T, client *etcd.Client) {
	t.Helper()
	etcdhelper.AssertKVs(t, client, `
%A
<<<<<
record/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:02.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:02.000Z,1.2.3.4,"{""key"":""value001""}","{""Content-Type"":""application/json""}","""---value001---"""
>>>>>

<<<<<
record/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:03.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:03.000Z,1.2.3.4,"{""key"":""value002""}","{""Content-Type"":""application/json""}","""---value002---"""
>>>>>

<<<<<
record/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:04.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:04.000Z,1.2.3.4,"{""key"":""value003""}","{""Content-Type"":""application/json""}","""---value003---"""
>>>>>

<<<<<
record/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:05.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:05.000Z,1.2.3.4,"{""key"":""value004""}","{""Content-Type"":""application/json""}","""---value004---"""
>>>>>

<<<<<
record/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:06.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:06.000Z,1.2.3.4,"{""key"":""value005""}","{""Content-Type"":""application/json""}","""---value005---"""
>>>>>

<<<<<
record/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:07.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:07.000Z,1.2.3.4,"{""key"":""value006""}","{""Content-Type"":""application/json""}","""---value006---"""
>>>>>

<<<<<
record/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:08.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:08.000Z,1.2.3.4,"{""key"":""value007""}","{""Content-Type"":""application/json""}","""---value007---"""
>>>>>

<<<<<
record/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:09.000Z_%s
-----
<<~~id~~>>,0001-01-01T00:00:09.000Z,1.2.3.4,"{""key"":""value008""}","{""Content-Type"":""application/json""}","""---value008---"""
>>>>>
%A
<<<<<
stats/received/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/my-api-1
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "0001-01-01T00:00:01.000Z",
  "sliceId": "0001-01-01T00:00:01.000Z",
  "count": 3,
  "size": 396,
  "lastRecordAt": "0001-01-01T00:00:04.000Z"
}
>>>>>

<<<<<
stats/received/00000123/my-receiver/my-export/0001-01-01T00:00:01.000Z/0001-01-01T00:00:01.000Z/my-api-2
-----
{
  "projectId": 123,
  "receiverId": "my-receiver",
  "exportId": "my-export",
  "fileId": "0001-01-01T00:00:01.000Z",
  "sliceId": "0001-01-01T00:00:01.000Z",
  "count": 5,
  "size": 660,
  "lastRecordAt": "0001-01-01T00:00:09.000Z"
}
>>>>>

`)
}
