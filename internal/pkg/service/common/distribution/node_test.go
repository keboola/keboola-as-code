package distribution_test

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestNodesDiscovery(t *testing.T) {
	t.Parallel()

	clk := clock.New() // use real clock

	etcdCfg := etcdhelper.TmpNamespace(t)
	client := etcdhelper.ClientForTest(t, etcdCfg)

	// Create 3 nodes and (pseudo) processes
	nodesCount := 3
	lock := &sync.Mutex{}
	nodes := make(map[int]*distribution.Node)
	loggers := make(map[int]log.DebugLogger)
	processes := make(map[int]*servicectx.Process)

	// Create nodes
	wg := &sync.WaitGroup{}
	for i := 0; i < nodesCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			node, d := createNode(t, clk, etcdCfg, fmt.Sprintf("node%d", i+1))
			if node != nil {
				lock.Lock()
				nodes[i] = node
				processes[i] = d.Process()
				loggers[i] = d.DebugLogger()
				lock.Unlock()
			}
		}()
	}
	wg.Wait()

	// Wait for initialization. All nodes must know about all nodes.
	assert.Eventually(t, func() bool {
		for _, node := range nodes {
			if !reflect.DeepEqual(node.Nodes(), []string{"node1", "node2", "node3"}) {
				return false
			}
		}
		return true
	}, 10*time.Second, 100*time.Millisecond)

	// Check tasks distribution.
	// Distribution is random, it depends on the hash function.
	// But all nodes return the same location for the task.
	for _, node := range nodes {
		assert.Equal(t, "node2", node.MustGetNodeFor("foo1"))
		assert.Equal(t, "node3", node.MustGetNodeFor("foo2"))
		assert.Equal(t, "node3", node.MustGetNodeFor("foo3"))
		assert.Equal(t, "node1", node.MustGetNodeFor("foo4"))
	}
	assert.True(t, nodes[0].MustCheckIsOwner("foo4"))
	assert.True(t, nodes[1].MustCheckIsOwner("foo1"))
	assert.True(t, nodes[2].MustCheckIsOwner("foo3"))
	assert.False(t, nodes[0].MustCheckIsOwner("foo3"))
	assert.False(t, nodes[1].MustCheckIsOwner("foo2"))
	assert.False(t, nodes[2].MustCheckIsOwner("foo1"))

	// Check etcd state
	etcdhelper.AssertKVsString(t, client, `
<<<<<
runtime/distribution/group/my-group/nodes/node1 (lease)
-----
node1
>>>>>

<<<<<
runtime/distribution/group/my-group/nodes/node2 (lease)
-----
node2
>>>>>

<<<<<
runtime/distribution/group/my-group/nodes/node3 (lease)
-----
node3
>>>>>
`)

	// Shutdown node1
	processes[0].Shutdown(context.Background(), errors.New("bye bye 1"))
	processes[0].WaitForShutdown()
	assert.Eventually(t, func() bool {
		return reflect.DeepEqual([]string{"node2", "node3"}, nodes[1].Nodes())
	}, time.Second, 10*time.Millisecond)
	assert.Eventually(t, func() bool {
		return reflect.DeepEqual([]string{"node2", "node3"}, nodes[2].Nodes())
	}, time.Second, 10*time.Millisecond)

	// Check etcd state
	etcdhelper.AssertKVsString(t, client, `
<<<<<
runtime/distribution/group/my-group/nodes/node2 (lease)
-----
node2
>>>>>

<<<<<
runtime/distribution/group/my-group/nodes/node3 (lease)
-----
node3
>>>>>
`)

	// Check tasks distribution
	for i := 1; i < nodesCount; i++ {
		assert.Equal(t, "node2", nodes[i].MustGetNodeFor("foo1"))
		assert.Equal(t, "node3", nodes[i].MustGetNodeFor("foo2"))
		assert.Equal(t, "node3", nodes[i].MustGetNodeFor("foo3"))
		assert.Equal(t, "node2", nodes[i].MustGetNodeFor("foo4")) // 1 -> 2
	}
	assert.True(t, nodes[1].MustCheckIsOwner("foo1"))
	assert.True(t, nodes[2].MustCheckIsOwner("foo3"))
	assert.False(t, nodes[1].MustCheckIsOwner("foo2"))
	assert.False(t, nodes[2].MustCheckIsOwner("foo1"))

	// Shutdown node2
	processes[1].Shutdown(context.Background(), errors.New("bye bye 2"))
	processes[1].WaitForShutdown()
	assert.Eventually(t, func() bool {
		return reflect.DeepEqual([]string{"node3"}, nodes[2].Nodes())
	}, time.Second, 10*time.Millisecond)

	// Check etcd state
	etcdhelper.AssertKVsString(t, client, `
<<<<<
runtime/distribution/group/my-group/nodes/node3 (lease)
-----
node3
>>>>>
`)

	// Check tasks distribution
	assert.Equal(t, "node3", nodes[2].MustGetNodeFor("foo1"))
	assert.Equal(t, "node3", nodes[2].MustGetNodeFor("foo2"))
	assert.Equal(t, "node3", nodes[2].MustGetNodeFor("foo3"))
	assert.Equal(t, "node3", nodes[2].MustGetNodeFor("foo4")) // 1 -> 2
	assert.True(t, nodes[2].MustCheckIsOwner("foo1"))
	assert.True(t, nodes[2].MustCheckIsOwner("foo2"))
	assert.True(t, nodes[2].MustCheckIsOwner("foo3"))
	assert.True(t, nodes[2].MustCheckIsOwner("foo4"))

	// Shutdown node3
	processes[2].Shutdown(context.Background(), errors.New("bye bye 3"))
	processes[2].WaitForShutdown()
	etcdhelper.AssertKVsString(t, client, "")

	// Logs differs in number of "the node ... gone" messages
	loggers[0].AssertJSONMessages(t, `
{"level":"info","message":"creating etcd session","node":"node1","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"created etcd session","node":"node1","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"registering the node \"node1\"","node":"node1","component":"distribution.my-group"}
{"level":"info","message":"the node \"node1\" registered","node":"node1","component":"distribution.my-group"}
{"level":"info","message":"watching for other nodes","node":"node1","component":"distribution.my-group"}
{"level":"info","message":"found a new node \"node%d\"","node":"node1","component":"distribution.my-group"}
{"level":"info","message":"found a new node \"node%d\"","node":"node1","component":"distribution.my-group"}
{"level":"info","message":"found a new node \"node%d\"","node":"node1","component":"distribution.my-group"}
{"level":"info","message":"exiting (bye bye 1)"}
{"level":"info","message":"received shutdown request","node":"node1","component":"distribution.my-group.listeners"}
{"level":"info","message":"shutdown done","node":"node1","component":"distribution.my-group.listeners"}
{"level":"info","message":"received shutdown request","node":"node1","component":"distribution.my-group"}
{"level":"info","message":"unregistering the node \"node1\"","node":"node1","component":"distribution.my-group"}
{"level":"info","message":"the node \"node1\" unregistered","node":"node1","component":"distribution.my-group"}
{"level":"info","message":"closing etcd session: context canceled","node":"node1","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"closed etcd session","node":"node1","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"shutdown done","node":"node1","component":"distribution.my-group"}
{"level":"info","message":"closing etcd connection","component":"etcd.client"}
{"level":"info","message":"closed etcd connection","component":"etcd.client"}
{"level":"info","message":"exited"}
`)

	loggers[1].AssertJSONMessages(t, `
{"level":"info","message":"creating etcd session","node":"node2","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"created etcd session","node":"node2","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"registering the node \"node2\"","node":"node2","component":"distribution.my-group"}
{"level":"info","message":"the node \"node2\" registered","node":"node2","component":"distribution.my-group"}
{"level":"info","message":"watching for other nodes","node":"node2","component":"distribution.my-group"}
{"level":"info","message":"found a new node \"node%d\"","node":"node2","component":"distribution.my-group"}
{"level":"info","message":"found a new node \"node%d\"","node":"node2","component":"distribution.my-group"}
{"level":"info","message":"found a new node \"node%d\"","node":"node2","component":"distribution.my-group"}
{"level":"info","message":"the node \"node1\" gone","node":"node2","component":"distribution.my-group"}
{"level":"info","message":"exiting (bye bye 2)"}
{"level":"info","message":"received shutdown request","node":"node2","component":"distribution.my-group.listeners"}
{"level":"info","message":"shutdown done","node":"node2","component":"distribution.my-group.listeners"}
{"level":"info","message":"received shutdown request","node":"node2","component":"distribution.my-group"}
{"level":"info","message":"unregistering the node \"node2\"","node":"node2","component":"distribution.my-group"}
{"level":"info","message":"the node \"node2\" unregistered","node":"node2","component":"distribution.my-group"}
{"level":"info","message":"closing etcd session: context canceled","node":"node2","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"closed etcd session","node":"node2","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"shutdown done","node":"node2","component":"distribution.my-group"}
{"level":"info","message":"closing etcd connection","component":"etcd.client"}
{"level":"info","message":"closed etcd connection","component":"etcd.client"}
{"level":"info","message":"exited"}
`)

	loggers[2].AssertJSONMessages(t, `
{"level":"info","message":"creating etcd session","node":"node3","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"created etcd session","node":"node3","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"registering the node \"node3\"","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"the node \"node3\" registered","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"watching for other nodes","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"found a new node \"node%d\"","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"found a new node \"node%d\"","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"found a new node \"node%d\"","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"the node \"node1\" gone","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"the node \"node2\" gone","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"exiting (bye bye 3)"}
{"level":"info","message":"received shutdown request","node":"node3","component":"distribution.my-group.listeners"}
{"level":"info","message":"shutdown done","node":"node3","component":"distribution.my-group.listeners"}
{"level":"info","message":"received shutdown request","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"unregistering the node \"node3\"","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"the node \"node3\" unregistered","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"closing etcd session: context canceled","node":"node3","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"closed etcd session","node":"node3","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"shutdown done","node":"node3","component":"distribution.my-group"}
{"level":"info","message":"closing etcd connection","component":"etcd.client"}
{"level":"info","message":"closed etcd connection","component":"etcd.client"}
{"level":"info","message":"exited"}
`)

	// All node are off, start a new node
	assert.Equal(t, 4, nodesCount+1)
	node4, d4 := createNode(t, clk, etcdCfg, "node4")
	process4 := d4.Process()
	assert.Eventually(t, func() bool {
		return reflect.DeepEqual([]string{"node4"}, node4.Nodes())
	}, time.Second, 10*time.Millisecond)

	// Check etcd state
	etcdhelper.AssertKVsString(t, client, `
<<<<<
runtime/distribution/group/my-group/nodes/node4 (lease)
-----
node4
>>>>>
`)
	// Shutdown node 4
	process4.Shutdown(context.Background(), errors.New("bye bye 4"))
	process4.WaitForShutdown()
	etcdhelper.AssertKVsString(t, client, "")

	d4.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"creating etcd session","node":"node4","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"created etcd session","node":"node4","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"registering the node \"node4\"","node":"node4","component":"distribution.my-group"}
{"level":"info","message":"the node \"node4\" registered","node":"node4","component":"distribution.my-group"}
{"level":"info","message":"watching for other nodes","node":"node4","component":"distribution.my-group"}
{"level":"info","message":"found a new node \"node4\"","node":"node4","component":"distribution.my-group"}
{"level":"info","message":"exiting (bye bye 4)"}
{"level":"info","message":"received shutdown request","node":"node4","component":"distribution.my-group.listeners"}
{"level":"info","message":"shutdown done","node":"node4","component":"distribution.my-group.listeners"}
{"level":"info","message":"received shutdown request","node":"node4","component":"distribution.my-group"}
{"level":"info","message":"unregistering the node \"node4\"","node":"node4","component":"distribution.my-group"}
{"level":"info","message":"the node \"node4\" unregistered","node":"node4","component":"distribution.my-group"}
{"level":"info","message":"closing etcd session: context canceled","node":"node4","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"closed etcd session","node":"node4","component":"distribution.my-group.etcd.session"}
{"level":"info","message":"shutdown done","node":"node4","component":"distribution.my-group"}
{"level":"info","message":"closing etcd connection","component":"etcd.client"}
{"level":"info","message":"closed etcd connection","component":"etcd.client"}
{"level":"info","message":"exited"}
`)
}

func createNode(t *testing.T, clk clock.Clock, etcdCfg etcdclient.Config, nodeID string) (*distribution.Node, dependencies.Mocked) {
	t.Helper()

	// Create dependencies
	d := createDeps(t, clk, nil, etcdCfg, nodeID)

	// Speedup tests with real clock,
	// and disable events grouping interval in tests with mocked clocks,
	// events will be processed immediately.
	groupInterval := 10 * time.Millisecond
	if _, ok := clk.(*clock.Mock); ok {
		groupInterval = 0
	}

	// Create node
	node, err := distribution.NewNode(
		nodeID, "my-group", d,
		distribution.WithStartupTimeout(time.Second),
		distribution.WithShutdownTimeout(time.Second),
		distribution.WithEventsGroupInterval(groupInterval),
	)
	assert.NoError(t, err)
	return node, d
}

func createDeps(t *testing.T, clk clock.Clock, logs io.Writer, etcdCfg etcdclient.Config, nodeID string) dependencies.Mocked {
	t.Helper()
	d := dependencies.NewMocked(
		t,
		dependencies.WithClock(clk),
		dependencies.WithNodeID(nodeID),
		dependencies.WithEtcdConfig(etcdCfg),
	)
	if logs != nil {
		d.DebugLogger().ConnectTo(logs)
	}
	return d
}
