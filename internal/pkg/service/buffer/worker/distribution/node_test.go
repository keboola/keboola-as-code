package distribution_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)

	// Create 3 nodes and (pseudo) processes
	nodesCount := 3
	lock := &sync.Mutex{}
	nodes := make(map[int]*Node)
	loggers := make(map[int]log.DebugLogger)
	processes := make(map[int]*servicectx.Process)

	createDeps := func(nodeNumber int) dependencies.Mocked {
		return dependencies.NewMockedDeps(
			t,
			dependencies.WithUniqueID(fmt.Sprintf("node%d", nodeNumber)),
			dependencies.WithLoggerPrefix(fmt.Sprintf("[node%d]", nodeNumber)),
			dependencies.WithCtx(ctx),
			dependencies.WithEtcdNamespace(etcdNamespace),
		)
	}

	// Create nodes
	wg := &sync.WaitGroup{}
	for i := 0; i < nodesCount; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := createDeps(i + 1)
			logger := d.DebugLogger()
			logger.ConnectTo(testhelper.VerboseStdout())
			process := d.Process()
			node, err := NewNode(d, WithStartupTimeout(time.Second), WithShutdownTimeout(time.Second))
			assert.NoError(t, err)

			lock.Lock()
			processes[i] = process
			nodes[i] = node
			loggers[i] = logger
			lock.Unlock()
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
	for i := 0; i < nodesCount; i++ {
		assert.Equal(t, "node2", nodes[i].MustGetNodeFor("foo1"))
		assert.Equal(t, "node3", nodes[i].MustGetNodeFor("foo2"))
		assert.Equal(t, "node3", nodes[i].MustGetNodeFor("foo3"))
		assert.Equal(t, "node1", nodes[i].MustGetNodeFor("foo4"))
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
runtime/workers/active/ids/node1 (lease=%d)
-----
node1
>>>>>

<<<<<
runtime/workers/active/ids/node2 (lease=%d)
-----
node2
>>>>>

<<<<<
runtime/workers/active/ids/node3 (lease=%d)
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
runtime/workers/active/ids/node2 (lease=%d)
-----
node2
>>>>>

<<<<<
runtime/workers/active/ids/node3 (lease=%d)
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
runtime/workers/active/ids/node3 (lease=%d)
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
[node1][distribution]INFO  creating etcd session
[node1][distribution]INFO  created etcd session | %s
[node1][distribution]INFO  registering the node "node1"
[node1][distribution]INFO  the node "node1" registered | %s
[node1][distribution]INFO  watching for other nodes
[node1][distribution]INFO  found a new node "node%d"
[node1][distribution]INFO  found a new node "node%d"
[node1][distribution]INFO  found a new node "node%d"
[node1]INFO  exiting (bye bye 1)
[node1][distribution]INFO  cancelled watcher
[node1][distribution]INFO  unregistering the node "node1"
[node1][distribution]INFO  the node "node1" unregistered | %s
[node1][distribution]INFO  closed etcd session
[node1]INFO  exited
`, loggers[0].AllMessages())
	wildcards.Assert(t, `
[node2]INFO  process unique id "node2"
[node2][distribution]INFO  creating etcd session
[node2][distribution]INFO  created etcd session | %s
[node2][distribution]INFO  registering the node "node2"
[node2][distribution]INFO  the node "node2" registered | %s
[node2][distribution]INFO  watching for other nodes
[node2][distribution]INFO  found a new node "node%d"
[node2][distribution]INFO  found a new node "node%d"
[node2][distribution]INFO  found a new node "node%d"
[node2][distribution]INFO  the node "node%d" gone
[node2]INFO  exiting (bye bye 2)
[node2][distribution]INFO  cancelled watcher
[node2][distribution]INFO  unregistering the node "node2"
[node2][distribution]INFO  the node "node2" unregistered | %s
[node2][distribution]INFO  closed etcd session
[node2]INFO  exited
`, loggers[1].AllMessages())
	wildcards.Assert(t, `
[node3]INFO  process unique id "node3"
[node3][distribution]INFO  creating etcd session
[node3][distribution]INFO  created etcd session | %s
[node3][distribution]INFO  registering the node "node3"
[node3][distribution]INFO  the node "node3" registered | %s
[node3][distribution]INFO  watching for other nodes
[node3][distribution]INFO  found a new node "node%d"
[node3][distribution]INFO  found a new node "node%d"
[node3][distribution]INFO  found a new node "node%d"
[node3][distribution]INFO  the node "node%d" gone
[node3][distribution]INFO  the node "node%d" gone
[node3]INFO  exiting (bye bye 3)
[node3][distribution]INFO  cancelled watcher
[node3][distribution]INFO  unregistering the node "node3"
[node3][distribution]INFO  the node "node3" unregistered | %s
[node3][distribution]INFO  closed etcd session
[node3]INFO  exited
`, loggers[2].AllMessages())

	// All node are off, start a new node
	assert.Equal(t, 4, nodesCount+1)
	d4 := createDeps(4)
	process4 := d4.Process()
	node4, err := NewNode(d4, WithStartupTimeout(time.Second), WithShutdownTimeout(time.Second))
	assert.NoError(t, err)
	assert.Eventually(t, func() bool {
		return reflect.DeepEqual([]string{"node4"}, node4.Nodes())
	}, time.Second, 10*time.Millisecond)

	// Check etcd state
	etcdhelper.AssertKVs(t, client, `
<<<<<
runtime/workers/active/ids/node4 (lease=%d)
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
[node4][distribution]INFO  creating etcd session
[node4][distribution]INFO  created etcd session | %s
[node4][distribution]INFO  registering the node "node4"
[node4][distribution]INFO  the node "node4" registered | %s
[node4][distribution]INFO  watching for other nodes
[node4][distribution]INFO  found a new node "node4"
[node4]INFO  exiting (bye bye 4)
[node4][distribution]INFO  cancelled watcher
[node4][distribution]INFO  unregistering the node "node4"
[node4][distribution]INFO  the node "node4" unregistered | %s
[node4][distribution]INFO  closed etcd session
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
