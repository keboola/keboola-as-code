package etcdop

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/tests/v3/integration"
	"google.golang.org/grpc/connectivity"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
)

// nolint:paralleltest // etcd integration tests cannot run in parallel, see integration.BeforeTestExternal
func TestWatchConsumer(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf(`etcd compact tests are tested only on Linux`)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	wg := &sync.WaitGroup{}
	logger := log.NewDebugLogger()

	// Create etcd cluster for test
	integration.BeforeTestExternal(t)
	cluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 3, UseBridge: true})
	defer cluster.Terminate(t)
	cluster.WaitLeader(t)
	testClient := cluster.Client(1)
	watchMember := cluster.Members[2]
	watchClient := cluster.Client(2)

	// Create consumer
	onCloseCalled := false
	pfx := prefixForTest()
	init := pfx.
		GetAllAndWatch(ctx, watchClient).
		SetupConsumer(logger).
		WithOnCreated(func(header *Header) {
			logger.Infof(ctx, `OnCreated: created (rev %v)`, header.Revision)
		}).
		WithOnRestarted(func(reason string, delay time.Duration) {
			logger.Infof(ctx, `OnRestarted: %s`, reason)
		}).
		WithOnError(func(err error) {
			if !strings.Contains(err.Error(), "mvcc: required revision has been compacted") {
				assert.Fail(t, "unexpected error", err)
			}
		}).
		WithOnClose(func(err error) {
			require.NoError(t, err)
			onCloseCalled = true
		}).
		WithForEach(func(events []WatchEvent, header *Header, restart bool) {
			var str strings.Builder
			for _, e := range events {
				str.WriteString(e.Type.String())
				str.WriteString(` "`)
				str.Write(e.Kv.Key)
				str.WriteString(`", `)
			}
			logger.Infof(ctx, `ForEach: restart=%t, events(%d): %s`, restart, len(events), strings.TrimSuffix(str.String(), ", "))
		}).
		StartConsumer(ctx, wg)

	// Wait for initialization
	require.NoError(t, <-init)

	// Expect created event
	logger.AssertJSONMessages(t, `{"level":"info","message":"OnCreated: created (rev 1)"}`)
	logger.Truncate()

	// Put some key
	require.NoError(t, pfx.Key("key1").Put(watchClient, "value1").Do(ctx).Err())

	// Expect forEach event
	assert.Eventually(t, func() bool {
		return strings.Count(logger.AllMessages(), "ForEach:") == 1
	}, 5*time.Second, 10*time.Millisecond)
	logger.AssertJSONMessages(t, `
{"level":"info","message":"ForEach: restart=false, events(1): create \"my/prefix/key1\""}
`)
	logger.Truncate()

	// Close watcher connection and block a new one
	watchMember.Bridge().PauseConnections()
	watchMember.Bridge().DropConnections()
	assert.Eventually(t, func() bool {
		return watchClient.ActiveConnection().GetState() == connectivity.Connecting
	}, 5*time.Second, 100*time.Millisecond)

	// Add some other keys, during the watcher is disconnected
	require.NoError(t, pfx.Key("key2").Put(testClient, "value2").Do(ctx).Err())
	require.NoError(t, pfx.Key("key3").Put(testClient, "value3").Do(ctx).Err())

	// Compact, during the watcher is disconnected
	status, err := testClient.Status(ctx, testClient.Endpoints()[0])
	require.NoError(t, err)
	_, err = testClient.Compact(ctx, status.Header.Revision)
	require.NoError(t, err)

	// Unblock dialer, watcher will be reconnected
	watchMember.Bridge().UnpauseConnections()

	// Expect restart event, followed with all 3 keys.
	// The restart flag is true.
	assert.Eventually(t, func() bool {
		return strings.Count(logger.AllMessages(), "my/prefix/key") == 3
	}, 5*time.Second, 10*time.Millisecond)
	logger.AssertJSONMessages(t, `
{"level":"warn","message":"watch error: etcdserver: mvcc: required revision has been compacted"}
{"level":"info","message":"restarted, backoff delay %s, reason: watch error: etcdserver: mvcc: required revision has been compacted"}
{"level":"info","message":"OnRestarted: backoff delay %s, reason: watch error: etcdserver: mvcc: required revision has been compacted"}
{"level":"info","message":"ForEach: restart=true, events(3): create \"my/prefix/key1\", create \"my/prefix/key2\", create \"my/prefix/key3\""}
`)
	logger.Truncate()

	// The restart flag is false in further events.
	require.NoError(t, pfx.Key("key4").Put(testClient, "value4").Do(ctx).Err())
	assert.Eventually(t, func() bool {
		return strings.Count(logger.AllMessages(), "ForEach:") == 1
	}, 5*time.Second, 10*time.Millisecond)
	logger.AssertJSONMessages(t, `
{"level":"info","message":"ForEach: restart=false, events(1): create \"my/prefix/key4\""}
`)
	logger.Truncate()

	// Stop
	cancel()
	wg.Wait()

	// OnClose callback must be called
	assert.True(t, onCloseCalled)
}
