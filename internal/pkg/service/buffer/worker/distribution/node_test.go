package distribution_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/lafikl/consistent"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestNodesDiscovery(t *testing.T) {
	t.Parallel()

	clk := clock.New() // use real clock
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)

	// Create 3 nodes and (pseudo) processes
	nodesCount := 3
	lock := &sync.Mutex{}
	nodes := make(map[int]*Node)
	loggers := make(map[int]log.DebugLogger)
	processes := make(map[int]*servicectx.Process)

	// Create nodes
	wg := &sync.WaitGroup{}
	for i := 0; i < nodesCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			node, d := createNode(t, clk, nil, etcdNamespace, fmt.Sprintf("node%d", i+1))
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
	}, 5*time.Second, 100*time.Millisecond)

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
	etcdhelper.AssertKVs(t, client, `
<<<<<
runtime/worker/node/active/id/node1 (lease=%d)
-----
node1
>>>>>

<<<<<
runtime/worker/node/active/id/node2 (lease=%d)
-----
node2
>>>>>

<<<<<
runtime/worker/node/active/id/node3 (lease=%d)
-----
node3
>>>>>
`)

	// Shutdown node1
	processes[0].Shutdown(errors.New("bye bye 1"))
	processes[0].WaitForShutdown()
	assert.Eventually(t, func() bool {
		return reflect.DeepEqual([]string{"node2", "node3"}, nodes[1].Nodes())
	}, time.Second, 10*time.Millisecond)
	assert.Eventually(t, func() bool {
		return reflect.DeepEqual([]string{"node2", "node3"}, nodes[2].Nodes())
	}, time.Second, 10*time.Millisecond)

	// Check etcd state
	etcdhelper.AssertKVs(t, client, `
<<<<<
runtime/worker/node/active/id/node2 (lease=%d)
-----
node2
>>>>>

<<<<<
runtime/worker/node/active/id/node3 (lease=%d)
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
	processes[1].Shutdown(errors.New("bye bye 2"))
	processes[1].WaitForShutdown()
	assert.Eventually(t, func() bool {
		return reflect.DeepEqual([]string{"node3"}, nodes[2].Nodes())
	}, time.Second, 10*time.Millisecond)

	// Check etcd state
	etcdhelper.AssertKVs(t, client, `
<<<<<
runtime/worker/node/active/id/node3 (lease=%d)
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
	processes[2].Shutdown(errors.New("bye bye 3"))
	processes[2].WaitForShutdown()
	etcdhelper.AssertKVs(t, client, "")

	// Logs differs in number of "the node ... gone" messages
	wildcards.Assert(t, `
[node1]INFO  process unique id "node1"
[node1][distribution][etcd-session]INFO  creating etcd session
[node1][distribution][etcd-session]INFO  created etcd session | %s
[node1][distribution]INFO  registering the node "node1"
[node1][distribution]INFO  the node "node1" registered | %s
[node1][distribution]INFO  watching for other nodes
[node1][distribution]INFO  found a new node "node%d"
[node1][distribution]INFO  found a new node "node%d"
[node1][distribution]INFO  found a new node "node%d"
[node1]INFO  exiting (bye bye 1)
[node1][distribution][listeners]INFO  received shutdown request
[node1][distribution][listeners]INFO  waiting for listeners
[node1][distribution][listeners]INFO  shutdown done
[node1][distribution]INFO  received shutdown request
[node1][distribution]INFO  unregistering the node "node1"
[node1][distribution]INFO  the node "node1" unregistered | %s
[node1][distribution]INFO  shutdown done
[node1][distribution][etcd-session]INFO  closing etcd session
[node1][distribution][etcd-session]INFO  closed etcd session | %s
[node1]INFO  exited
`, loggers[0].AllMessages())
	wildcards.Assert(t, `
[node2]INFO  process unique id "node2"
[node2][distribution][etcd-session]INFO  creating etcd session
[node2][distribution][etcd-session]INFO  created etcd session | %s
[node2][distribution]INFO  registering the node "node2"
[node2][distribution]INFO  the node "node2" registered | %s
[node2][distribution]INFO  watching for other nodes
[node2][distribution]INFO  found a new node "node%d"
[node2][distribution]INFO  found a new node "node%d"
[node2][distribution]INFO  found a new node "node%d"
[node2][distribution]INFO  the node "node%d" gone
[node2]INFO  exiting (bye bye 2)
[node2][distribution][listeners]INFO  received shutdown request
[node2][distribution][listeners]INFO  waiting for listeners
[node2][distribution][listeners]INFO  shutdown done
[node2][distribution]INFO  received shutdown request
[node2][distribution]INFO  unregistering the node "node2"
[node2][distribution]INFO  the node "node2" unregistered | %s
[node2][distribution]INFO  shutdown done
[node2][distribution][etcd-session]INFO  closing etcd session
[node2][distribution][etcd-session]INFO  closed etcd session | %s
[node2]INFO  exited
`, loggers[1].AllMessages())
	wildcards.Assert(t, `
[node3]INFO  process unique id "node3"
[node3][distribution][etcd-session]INFO  creating etcd session
[node3][distribution][etcd-session]INFO  created etcd session | %s
[node3][distribution]INFO  registering the node "node3"
[node3][distribution]INFO  the node "node3" registered | %s
[node3][distribution]INFO  watching for other nodes
[node3][distribution]INFO  found a new node "node%d"
[node3][distribution]INFO  found a new node "node%d"
[node3][distribution]INFO  found a new node "node%d"
[node3][distribution]INFO  the node "node%d" gone
[node3][distribution]INFO  the node "node%d" gone
[node3]INFO  exiting (bye bye 3)
[node3][distribution][listeners]INFO  received shutdown request
[node3][distribution][listeners]INFO  waiting for listeners
[node3][distribution][listeners]INFO  shutdown done
[node3][distribution]INFO  received shutdown request
[node3][distribution]INFO  unregistering the node "node3"
[node3][distribution]INFO  the node "node3" unregistered | %s
[node3][distribution]INFO  shutdown done
[node3][distribution][etcd-session]INFO  closing etcd session
[node3][distribution][etcd-session]INFO  closed etcd session | %s
[node3]INFO  exited
`, loggers[2].AllMessages())

	// All node are off, start a new node
	assert.Equal(t, 4, nodesCount+1)
	node4, d4 := createNode(t, clk, nil, etcdNamespace, "node4")
	process4 := d4.Process()
	assert.Eventually(t, func() bool {
		return reflect.DeepEqual([]string{"node4"}, node4.Nodes())
	}, time.Second, 10*time.Millisecond)

	// Check etcd state
	etcdhelper.AssertKVs(t, client, `
<<<<<
runtime/worker/node/active/id/node4 (lease=%d)
-----
node4
>>>>>
`)
	// Shutdown node 4
	process4.Shutdown(errors.New("bye bye 4"))
	process4.WaitForShutdown()
	etcdhelper.AssertKVs(t, client, "")

	wildcards.Assert(t, `
[node4]INFO  process unique id "node4"
[node4][distribution][etcd-session]INFO  creating etcd session
[node4][distribution][etcd-session]INFO  created etcd session | %s
[node4][distribution]INFO  registering the node "node4"
[node4][distribution]INFO  the node "node4" registered | %s
[node4][distribution]INFO  watching for other nodes
[node4][distribution]INFO  found a new node "node4"
[node4]INFO  exiting (bye bye 4)
[node4][distribution][listeners]INFO  received shutdown request
[node4][distribution][listeners]INFO  waiting for listeners
[node4][distribution][listeners]INFO  shutdown done
[node4][distribution]INFO  received shutdown request
[node4][distribution]INFO  unregistering the node "node4"
[node4][distribution]INFO  the node "node4" unregistered | %s
[node4][distribution]INFO  shutdown done
[node4][distribution][etcd-session]INFO  closing etcd session
[node4][distribution][etcd-session]INFO  closed etcd session | %s
[node4]INFO  exited
`, d4.DebugLogger().AllMessages())
}

// TestConsistentHashLib tests the library behavior and shows how it should be used.
func TestConsistentHashLib(t *testing.T) {
	t.Parallel()
	c := consistent.New()

	// Test no node
	_, err := c.Get("foo")
	assert.Error(t, err)
	assert.Equal(t, consistent.ErrNoHosts, err)

	// Add nodes
	c.Add("node1")
	c.Add("node2")
	c.Add("node3")
	c.Add("node4")
	c.Add("node5")

	// Check distribution of the keys in 5 nodes
	keysPerNode := make(map[string]int)
	for i := 1; i <= 100; i++ {
		node, err := c.Get(fmt.Sprintf("foo%02d", i))
		assert.NoError(t, err)
		keysPerNode[node] = keysPerNode[node] + 1
	}
	assert.Equal(t, map[string]int{
		"node1": 27,
		"node2": 26,
		"node3": 13,
		"node4": 24,
		"node5": 10,
	}, keysPerNode)

	// Delete nodes
	c.Remove("node2")
	c.Remove("node4")

	// Check distribution of the keys in 3 nodes
	keysPerNode = make(map[string]int)
	for i := 1; i <= 100; i++ {
		node, err := c.Get(fmt.Sprintf("foo%02d", i))
		assert.NoError(t, err)
		keysPerNode[node] = keysPerNode[node] + 1
	}
	assert.Equal(t, map[string]int{
		"node1": 47,
		"node3": 30,
		"node5": 23,
	}, keysPerNode)
}

func createNode(t *testing.T, clk clock.Clock, logs io.Writer, etcdNamespace, nodeName string) (*Node, dependencies.Mocked) {
	t.Helper()

	// Create dependencies
	d := createDeps(t, clk, logs, etcdNamespace, nodeName)

	// Disable waiting for self-discovery in tests with mocked clocks
	selfDiscoveryTimeout := time.Second
	if _, ok := clk.(*clock.Mock); ok {
		selfDiscoveryTimeout = 0
	}

	// Disable events grouping interval in tests with mocked clocks,
	// events will be processed immediately.
	groupInterval := 10 * time.Millisecond // speedup tests with real clock
	if _, ok := clk.(*clock.Mock); ok {
		groupInterval = 0
	}

	// Create node
	node, err := NewNode(
		d,
		WithStartupTimeout(time.Second),
		WithShutdownTimeout(time.Second),
		WithSelfDiscoveryTimeout(selfDiscoveryTimeout),
		WithEventsGroupInterval(groupInterval),
	)
	assert.NoError(t, err)
	return node, d
}

func createDeps(t *testing.T, clk clock.Clock, logs io.Writer, etcdNamespace, nodeName string) dependencies.Mocked {
	t.Helper()
	d := dependencies.NewMockedDeps(
		t,
		dependencies.WithClock(clk),
		dependencies.WithUniqueID(nodeName),
		dependencies.WithLoggerPrefix(fmt.Sprintf("[%s]", nodeName)),
		dependencies.WithEtcdNamespace(etcdNamespace),
	)
	if logs != nil {
		d.DebugLogger().ConnectTo(logs)
	}
	d.DebugLogger().ConnectTo(testhelper.VerboseStdout())
	return d
}
