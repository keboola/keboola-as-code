package distribution_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

const resetInterval = time.Minute // only for tests

func TestDistributedWork(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()

	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)

	logsPerNode := make(map[string]*ioutil.AtomicWriter)
	createNodeWithWork := func(nodeName string) (*Node, dependencies.Mocked, *atomic.Int64) {
		logs := ioutil.NewAtomicWriter()
		logsPerNode[nodeName] = logs
		node, d := createNode(t, clk, logs, etcdNamespace, nodeName)

		// Start distributed work
		ctx, cancel := context.WithCancel(context.Background())
		wg := &sync.WaitGroup{}
		restartCount := startWork(t, node, ctx, wg, d.Logger().AddPrefix("[work]"))
		d.Process().OnShutdown(func() {
			cancel()
			wg.Wait()
		})
		return node, d, restartCount
	}

	// Create node 1
	node1, d1, restartCount1 := createNodeWithWork("node1")
	assertAllTasksAssignedOnce(t, node1)

	// 3x reset
	clk.Add(resetInterval)
	assertAllTasksAssignedOnce(t, node1)
	clk.Add(resetInterval)
	assertAllTasksAssignedOnce(t, node1)
	clk.Add(resetInterval)
	assertAllTasksAssignedOnce(t, node1)

	// Stop node 1
	assert.Eventually(t, func() bool {
		return restartCount1.Load() == 3
	}, time.Second, 10*time.Millisecond, "timeout")
	etcdhelper.ExpectModification(t, client, func() {
		d1.Process().Shutdown(errors.New("bye bye 1"))
		d1.Process().WaitForShutdown()
	})

	// Create node 2
	node2, d2, restartCount2 := createNodeWithWork("node2")
	assertAllTasksAssignedOnce(t, node2)

	// Create node 3
	node3, d3, restartCount3 := createNodeWithWork("node3")
	assert.Eventually(t, func() bool {
		return node2.HasNode("node3") && restartCount2.Load() == 1
	}, time.Second, 10*time.Millisecond, "timeout")
	assertAllTasksAssignedOnce(t, node2, node3)

	// Create node 4
	node4, d4, restartCount4 := createNodeWithWork("node4")
	assert.Eventually(t, func() bool {
		return node2.HasNode("node4") && restartCount2.Load() == 2
	}, time.Second, 10*time.Millisecond, "timeout")
	assert.Eventually(t, func() bool {
		return node3.HasNode("node4") && restartCount3.Load() == 1
	}, time.Second, 10*time.Millisecond, "timeout")
	assertAllTasksAssignedOnce(t, node2, node3, node4)

	// Shutdown node 3
	d3.Process().Shutdown(errors.New("bye bye 3"))
	d3.Process().WaitForShutdown()
	assert.Eventually(t, func() bool {
		return !node2.HasNode("node3") && restartCount2.Load() == 3
	}, time.Second, 10*time.Millisecond, "timeout")
	assert.Eventually(t, func() bool {
		return !node4.HasNode("node3") && restartCount4.Load() == 1
	}, time.Second, 10*time.Millisecond, "timeout")
	assertAllTasksAssignedOnce(t, node2, node4)

	// Shutdown node 4
	d4.Process().Shutdown(errors.New("bye bye 4"))
	d4.Process().WaitForShutdown()
	assert.Eventually(t, func() bool {
		return !node2.HasNode("node4") && restartCount2.Load() == 4
	}, time.Second, 10*time.Millisecond, "timeout")
	assertAllTasksAssignedOnce(t, node2)

	// Shutdown node 2
	etcdhelper.ExpectModification(t, client, func() {
		d2.Process().Shutdown(errors.New("bye bye 2"))
		d2.Process().WaitForShutdown()
	})

	// Check logs
	expected1 := `
%A
[node1][work]INFO  ready
[node1][work]INFO  assigned tasks: 1,2,3,4,5,6,7,8,9,10
[node1][work]INFO  restart: periodical
[node1][work]INFO  assigned tasks: 1,2,3,4,5,6,7,8,9,10
[node1][work]INFO  restart: periodical
[node1][work]INFO  assigned tasks: 1,2,3,4,5,6,7,8,9,10
[node1][work]INFO  restart: periodical
[node1][work]INFO  assigned tasks: 1,2,3,4,5,6,7,8,9,10
[node1]INFO  exiting (bye bye 1)
%A
`
	expected2 := `
%A
[node2][work]INFO  ready
[node2][work]INFO  assigned tasks: 1,2,3,4,5,6,7,8,9,10
[node2][distribution]INFO  found a new node "node3"
[node2][work]INFO  restart: distribution changed: found a new node "node3"
[node2][work]INFO  assigned tasks: 4,7,8,9,10
[node2][distribution]INFO  found a new node "node4"
[node2][work]INFO  restart: distribution changed: found a new node "node4"
[node2][work]INFO  assigned tasks: 4,8,9,10
[node2][distribution]INFO  the node "node3" gone
[node2][work]INFO  restart: distribution changed: the node "node3" gone
[node2][work]INFO  assigned tasks: 1,3,4,6,8,9,10
[node2][distribution]INFO  the node "node4" gone
[node2][work]INFO  restart: distribution changed: the node "node4" gone
[node2][work]INFO  assigned tasks: 1,2,3,4,5,6,7,8,9,10
[node2]INFO  exiting (bye bye 2)
%A
`
	expected3 := `
%A
[node3][work]INFO  ready
[node3][work]INFO  assigned tasks: 1,2,3,5,6
[node3][distribution]INFO  found a new node "node4"
[node3][work]INFO  restart: distribution changed: found a new node "node4"
[node3][work]INFO  assigned tasks: 1,3,6
[node3]INFO  exiting (bye bye 3)
%A
`
	expected4 := `
%A
[node4][work]INFO  ready
[node4][work]INFO  assigned tasks: 2,5,7
[node4][distribution]INFO  the node "node3" gone
[node4][work]INFO  restart: distribution changed: the node "node3" gone
[node4][work]INFO  assigned tasks: 2,5,7
[node4]INFO  exiting (bye bye 4)
%A
`
	wildcards.Assert(t, expected1, logsPerNode["node1"].String())
	wildcards.Assert(t, expected2, logsPerNode["node2"].String())
	wildcards.Assert(t, expected3, logsPerNode["node3"].String())
	wildcards.Assert(t, expected4, logsPerNode["node4"].String())
}

func startWork(t *testing.T, node *Node, ctx context.Context, wg *sync.WaitGroup, logger log.Logger) *atomic.Int64 {
	t.Helper()
	restarts := atomic.NewInt64(-1) // -1, first call is start, not restart
	work := func(ctx context.Context, assigner *Assigner) <-chan error {
		initDone := make(chan error)

		wg.Add(1)
		go func() {
			defer wg.Done()

			// Check distribution of 10 tasks
			var ownedTasks []string
			for i := 1; i <= 10; i++ {
				// Check if the node is owner of the task
				// Error is not expected, node had to discover at least itself
				taskName := fmt.Sprintf("task%02d", i)
				if assigner.MustCheckIsOwner(taskName) {
					ownedTasks = append(ownedTasks, strconv.Itoa(i))
				}
			}

			logger.Infof("assigned tasks: %s", strings.Join(ownedTasks, ","))
			close(initDone)
			restarts.Inc()
			<-ctx.Done()
		}()

		return initDone
	}
	assert.NoError(t, <-node.StartWork(ctx, wg, logger, work, WithResetInterval(resetInterval)))
	return restarts
}

func assertAllTasksAssignedOnce(t *testing.T, nodes ...*Node) {
	t.Helper()
	taskToNode := make(map[int]string)
	for _, node := range nodes {
		for i := 1; i <= 10; i++ {
			taskName := fmt.Sprintf("task%02d", i)
			if node.MustCheckIsOwner(taskName) {
				if v, found := taskToNode[i]; found {
					assert.Fail(t, fmt.Sprintf(`task %d is assigned to multiple nodes: %s; %s`, i, v, node.NodeID()))
				}
				taskToNode[i] = node.NodeID()
			}
		}
	}
	for i := 1; i <= 10; i++ {
		if _, found := taskToNode[i]; !found {
			assert.Fail(t, fmt.Sprintf(`task %d is not assigned to any node`, i))
		}
	}
}
