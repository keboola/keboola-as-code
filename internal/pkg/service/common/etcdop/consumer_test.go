package etcdop

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"
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
			logger.InfofCtx(ctx, `OnCreated: created (rev %v)`, header.Revision)
		}).
		WithOnRestarted(func(reason string, delay time.Duration) {
			logger.InfofCtx(ctx, `OnRestarted: %s`, reason)
		}).
		WithOnError(func(err error) {
			if !strings.Contains(err.Error(), "mvcc: required revision has been compacted") {
				assert.Fail(t, "unexpected error", err)
			}
		}).
		WithOnClose(func(err error) {
			assert.NoError(t, err)
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
			logger.InfofCtx(ctx, `ForEach: restart=%t, events(%d): %s`, restart, len(events), strings.TrimSuffix(str.String(), ", "))
		}).
		StartConsumer(ctx, wg)

	// Wait for initialization
	assert.NoError(t, <-init)

	// Expect created event
	wildcards.Assert(t, "INFO  OnCreated: created (rev %d)", logger.AllMessages())
	logger.Truncate()

	// Put some key
	assert.NoError(t, pfx.Key("key1").Put("value1").Do(ctx, watchClient))

	// Expect forEach event
	assert.Eventually(t, func() bool {
		return strings.Count(logger.AllMessages(), "ForEach:") == 1
	}, 5*time.Second, 10*time.Millisecond)
	wildcards.Assert(t, `
INFO  ForEach: restart=false, events(1): create "my/prefix/key1"
`, logger.AllMessages())
	logger.Truncate()

	// Close watcher connection and block a new one
	watchMember.Bridge().PauseConnections()
	watchMember.Bridge().DropConnections()
	assert.Eventually(t, func() bool {
		return watchClient.ActiveConnection().GetState() == connectivity.Connecting
	}, 5*time.Second, 100*time.Millisecond)

	// Add some other keys, during the watcher is disconnected
	assert.NoError(t, pfx.Key("key2").Put("value2").Do(ctx, testClient))
	assert.NoError(t, pfx.Key("key3").Put("value3").Do(ctx, testClient))

	// Compact, during the watcher is disconnected
	status, err := testClient.Status(ctx, testClient.Endpoints()[0])
	assert.NoError(t, err)
	_, err = testClient.Compact(ctx, status.Header.Revision)
	assert.NoError(t, err)

	// Unblock dialer, watcher will be reconnected
	watchMember.Bridge().UnpauseConnections()

	// Expect restart event, followed with all 3 keys.
	// The restart flag is true.
	assert.Eventually(t, func() bool {
		return strings.Count(logger.AllMessages(), "my/prefix/key") == 3
	}, 5*time.Second, 10*time.Millisecond)
	wildcards.Assert(t, `
WARN  watch error: etcdserver: mvcc: required revision has been compacted
INFO  restarted, backoff delay %s, reason: watch error: etcdserver: mvcc: required revision has been compacted
INFO  OnRestarted: backoff delay %s, reason: watch error: etcdserver: mvcc: required revision has been compacted
INFO  ForEach: restart=true, events(3): create "my/prefix/key1", create "my/prefix/key2", create "my/prefix/key3"
`, logger.AllMessages())
	logger.Truncate()

	// The restart flag is false in further events.
	assert.NoError(t, pfx.Key("key4").Put("value4").Do(ctx, testClient))
	assert.Eventually(t, func() bool {
		return strings.Count(logger.AllMessages(), "ForEach:") == 1
	}, 5*time.Second, 10*time.Millisecond)
	wildcards.Assert(t, `
INFO  ForEach: restart=false, events(1): create "my/prefix/key4"
`, logger.AllMessages())
	logger.Truncate()

	// Stop
	cancel()
	wg.Wait()

	// OnClose callback must be called
	assert.True(t, onCloseCalled)
}
