package etcdop

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/tests/v3/integration"
	"google.golang.org/grpc/connectivity"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// nolint:paralleltest // etcd integration tests cannot run in parallel, see integration.BeforeTestExternal
func TestWatchConsumer_NotTyped(t *testing.T) {
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
	consumer := pfx.
		GetAllAndWatch(ctx, watchClient).
		SetupConsumer().
		WithOnCreated(func(header *Header) {
			logger.Infof(ctx, `OnCreated: created (rev %v)`, header.Revision)
		}).
		WithOnRestarted(func(cause error, delay time.Duration) {
			logger.Infof(ctx, `OnRestarted: %s`, cause)
		}).
		WithOnError(func(err error) {
			if !strings.Contains(err.Error(), "mvcc: required revision has been compacted") {
				assert.Fail(t, "unexpected error", err)
			}
		}).
		WithOnClose(func(err error) {
			assert.ErrorIs(t, err, context.Canceled) // there should be no unexpected error
			onCloseCalled = true
		}).
		WithForEach(func(events []WatchEvent[[]byte], header *Header, restart bool) {
			var str strings.Builder
			for _, e := range events {
				str.WriteString(e.Type.String())
				str.WriteString(` "`)
				str.Write(e.Kv.Key)
				str.WriteString(`", `)
			}
			logger.Infof(ctx, `ForEach: restart=%t, events(%d): %s`, restart, len(events), strings.TrimSuffix(str.String(), ", "))
		}).
		BuildConsumer()

	// Wait for initialization
	assert.NoError(t, <-consumer.StartConsumer(ctx, wg, logger))

	// Expect created event
	logger.AssertJSONMessages(t, `{"level":"info","message":"OnCreated: created (rev 1)"}`)
	logger.Truncate()

	// Put some key
	assert.NoError(t, pfx.Key("key1").Put(watchClient, "value1").Do(ctx).Err())

	// Expect forEach event
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, strings.Count(logger.AllMessages(), "ForEach:"))
	}, 5*time.Second, 10*time.Millisecond)
	logger.AssertJSONMessages(t, `
{"level":"info","message":"ForEach: restart=false, events(1): create \"my/prefix/key1\""}
`)
	logger.Truncate()

	// Close watcher connection and block a new one
	watchMember.Bridge().PauseConnections()
	watchMember.Bridge().DropConnections()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, connectivity.Connecting, watchClient.ActiveConnection().GetState())
	}, 5*time.Second, 100*time.Millisecond)

	// Add some other keys, during the watcher is disconnected
	assert.NoError(t, pfx.Key("key2").Put(testClient, "value2").Do(ctx).Err())
	assert.NoError(t, pfx.Key("key3").Put(testClient, "value3").Do(ctx).Err())

	// Compact, during the watcher is disconnected
	status, err := testClient.Status(ctx, testClient.Endpoints()[0])
	assert.NoError(t, err)
	_, err = testClient.Compact(ctx, status.Header.Revision)
	assert.NoError(t, err)

	// Unblock dialer, watcher will be reconnected
	watchMember.Bridge().UnpauseConnections()

	// Expect restart event, followed with all 3 keys.
	// The restart flag is true.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"warn","message":"watch error: etcdserver: mvcc: required revision has been compacted"}
{"level":"info","message":"watch stream consumer restarted: unexpected restart, backoff delay %s, cause:\n- watch error: etcdserver: mvcc: required revision has been compacted"}
{"level":"info","message":"OnRestarted: unexpected restart, backoff delay %s, cause:\n- watch error: etcdserver: mvcc: required revision has been compacted"}
{"level":"info","message":"ForEach: restart=true, events(3): create \"my/prefix/key1\", create \"my/prefix/key2\", create \"my/prefix/key3\""}
{"level":"info","message":"watch stream created"}                                                                                
{"level":"info","message":"OnCreated: created (rev %d)"}
`)
	}, 5*time.Second, 10*time.Millisecond)
	logger.Truncate()

	// The restart flag is false in further events.
	assert.NoError(t, pfx.Key("key4").Put(testClient, "value4").Do(ctx).Err())
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"info","message":"ForEach: restart=false, events(1): create \"my/prefix/key4\""}
`)
	}, 5*time.Second, 10*time.Millisecond)
	logger.Truncate()

	// Test manual restart
	consumer.Restart(errors.New("test restart"))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"info","message":"watch stream consumer restarted: test restart"}                                                               
{"level":"info","message":"OnRestarted: test restart"}                                                                      
{"level":"info","message":"ForEach: restart=true, events(4): create \"my/prefix/key1\", create \"my/prefix/key2\", create \"my/prefix/key3\", create \"my/prefix/key4\""}                                                                                                                                                 
{"level":"info","message":"watch stream created"}                                                                                
{"level":"info","message":"OnCreated: created (rev %d)"}
`)
	}, 5*time.Second, 10*time.Millisecond)
	logger.Truncate()

	// Stop
	cancel()
	wg.Wait()

	// OnClose callback must be called
	assert.True(t, onCloseCalled)
}

// nolint:paralleltest // etcd integration tests cannot run in parallel, see integration.BeforeTestExternal
func TestWatchConsumer_Typed(t *testing.T) {
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
	pfx := typedPrefixForTest()
	consumer := pfx.
		GetAllAndWatch(ctx, watchClient).
		SetupConsumer().
		WithOnCreated(func(header *Header) {
			logger.Infof(ctx, `OnCreated: created (rev %v)`, header.Revision)
		}).
		WithOnRestarted(func(cause error, delay time.Duration) {
			logger.Infof(ctx, `OnRestarted: %s`, cause)
		}).
		WithOnError(func(err error) {
			if !strings.Contains(err.Error(), "mvcc: required revision has been compacted") {
				assert.Fail(t, "unexpected error", err)
			}
		}).
		WithOnClose(func(err error) {
			assert.ErrorIs(t, err, context.Canceled) // there should be no unexpected error
			onCloseCalled = true
		}).
		WithForEach(func(events []WatchEvent[fooType], header *Header, restart bool) {
			var str strings.Builder
			for _, e := range events {
				str.WriteString(e.Type.String())
				str.WriteString(` "`)
				str.Write(e.Kv.Key)
				str.WriteString(`", `)
			}
			logger.Infof(ctx, `ForEach: restart=%t, events(%d): %s`, restart, len(events), strings.TrimSuffix(str.String(), ", "))
		}).
		BuildConsumer()

	// Wait for initialization
	assert.NoError(t, <-consumer.StartConsumer(ctx, wg, logger))

	// Expect created event
	logger.AssertJSONMessages(t, `{"level":"info","message":"OnCreated: created (rev 1)"}`)
	logger.Truncate()

	// Put some key
	assert.NoError(t, pfx.Key("key1").Put(watchClient, "value1").Do(ctx).Err())

	// Expect forEach event
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 1, strings.Count(logger.AllMessages(), "ForEach:"))
	}, 5*time.Second, 10*time.Millisecond)
	logger.AssertJSONMessages(t, `
{"level":"info","message":"ForEach: restart=false, events(1): create \"my/prefix/key1\""}
`)
	logger.Truncate()

	// Close watcher connection and block a new one
	watchMember.Bridge().PauseConnections()
	watchMember.Bridge().DropConnections()
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, connectivity.Connecting, watchClient.ActiveConnection().GetState())
	}, 5*time.Second, 100*time.Millisecond)

	// Add some other keys, during the watcher is disconnected
	assert.NoError(t, pfx.Key("key2").Put(testClient, "value2").Do(ctx).Err())
	assert.NoError(t, pfx.Key("key3").Put(testClient, "value3").Do(ctx).Err())

	// Compact, during the watcher is disconnected
	status, err := testClient.Status(ctx, testClient.Endpoints()[0])
	assert.NoError(t, err)
	_, err = testClient.Compact(ctx, status.Header.Revision)
	assert.NoError(t, err)

	// Unblock dialer, watcher will be reconnected
	watchMember.Bridge().UnpauseConnections()

	// Expect restart event, followed with all 3 keys.
	// The restart flag is true.
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"warn","message":"watch error: etcdserver: mvcc: required revision has been compacted"}
{"level":"info","message":"watch stream consumer restarted: unexpected restart, backoff delay %s, cause:\n- watch error: etcdserver: mvcc: required revision has been compacted"}
{"level":"info","message":"OnRestarted: unexpected restart, backoff delay %s, cause:\n- watch error: etcdserver: mvcc: required revision has been compacted"}
{"level":"info","message":"ForEach: restart=true, events(3): create \"my/prefix/key1\", create \"my/prefix/key2\", create \"my/prefix/key3\""}
{"level":"info","message":"watch stream created"}                                                                                
{"level":"info","message":"OnCreated: created (rev %d)"}
`)
	}, 5*time.Second, 10*time.Millisecond)
	logger.Truncate()

	// The restart flag is false in further events.
	assert.NoError(t, pfx.Key("key4").Put(testClient, "value4").Do(ctx).Err())
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"info","message":"ForEach: restart=false, events(1): create \"my/prefix/key4\""}
`)
	}, 5*time.Second, 10*time.Millisecond)
	logger.Truncate()

	// Test manual restart
	consumer.Restart(errors.New("test restart"))
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"info","message":"watch stream consumer restarted: test restart"}                                                               
{"level":"info","message":"OnRestarted: test restart"}                                                                      
{"level":"info","message":"ForEach: restart=true, events(4): create \"my/prefix/key1\", create \"my/prefix/key2\", create \"my/prefix/key3\", create \"my/prefix/key4\""}                                                                                                                                                 
{"level":"info","message":"watch stream created"}                                                                                
{"level":"info","message":"OnCreated: created (rev %d)"}
`)
	}, 5*time.Second, 10*time.Millisecond)
	logger.Truncate()

	// Stop
	cancel()
	wg.Wait()

	// OnClose callback must be called
	assert.True(t, onCloseCalled)
}
