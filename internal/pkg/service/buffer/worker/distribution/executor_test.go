package distribution_test

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

const resetInterval = time.Minute // only for tests

func TestDistributedExecutor(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()

	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)

	logsPerNode := make(map[string]*ioutil.Writer)
	createNode := func(nodeName string) (*Node, dependencies.Mocked) {
		logs := ioutil.NewBufferedWriter()
		logsPerNode[nodeName] = logs
		return createNodeWithExecutor(t, clk, logs, etcdNamespace, nodeName)
	}

	// Create node 1
	node1, d1 := createNode("node1")
	assertAllTasksAssignedOnce(t, node1)

	// 3x reset
	clk.Add(resetInterval)
	assertAllTasksAssignedOnce(t, node1)
	clk.Add(resetInterval)
	assertAllTasksAssignedOnce(t, node1)
	clk.Add(resetInterval)
	assertAllTasksAssignedOnce(t, node1)

	// Stop node 1
	etcdhelper.ExpectModification(t, client, func() {
		d1.Process().Shutdown(errors.New("bye bye 1"))
		d1.Process().WaitForShutdown()
	})

	// Create node 2
	node2, d2 := createNode("node2")
	assertAllTasksAssignedOnce(t, node2)

	// Create node 3
	node3, d3 := createNode("node3")
	assert.Eventually(t, func() bool {
		return node2.HasNode("node3")
	}, 5*time.Second, 10*time.Millisecond, "timeout")
	assertAllTasksAssignedOnce(t, node2, node3)

	// Create node 4
	node4, d4 := createNode("node4")
	assert.Eventually(t, func() bool {
		return node2.HasNode("node4")
	}, 5*time.Second, 10*time.Millisecond, "timeout")
	assert.Eventually(t, func() bool {
		return node3.HasNode("node4")
	}, 5*time.Second, 10*time.Millisecond, "timeout")
	assertAllTasksAssignedOnce(t, node2, node3, node4)

	// Shutdown node 3
	d3.Process().Shutdown(errors.New("bye bye 3"))
	d3.Process().WaitForShutdown()
	assert.Eventually(t, func() bool {
		return !node2.HasNode("node3")
	}, 5*time.Second, 10*time.Millisecond, "timeout")
	assert.Eventually(t, func() bool {
		return !node4.HasNode("node3")
	}, 5*time.Second, 10*time.Millisecond, "timeout")
	assertAllTasksAssignedOnce(t, node2, node4)

	// Shutdown node 4
	d4.Process().Shutdown(errors.New("bye bye 4"))
	d4.Process().WaitForShutdown()
	assert.Eventually(t, func() bool {
		return !node2.HasNode("node4")
	}, 5*time.Second, 10*time.Millisecond, "timeout")
	assertAllTasksAssignedOnce(t, node2)

	// Shutdown node 2
	etcdhelper.ExpectModification(t, client, func() {
		d2.Process().Shutdown(errors.New("bye bye 2"))
		d2.Process().WaitForShutdown()
	})

	// Check logs
	expected1 := `
%A
[node1][distribution][my-executor]INFO  reset: initialization
[node1][distribution][my-executor][work]INFO  I am owning tasks: 1,2,3,4,5,6,7,8,9,10
[node1][distribution][my-executor]INFO  reset: periodical
[node1][distribution][my-executor][work]INFO  I am owning tasks: 1,2,3,4,5,6,7,8,9,10
[node1][distribution][my-executor]INFO  reset: periodical
[node1][distribution][my-executor][work]INFO  I am owning tasks: 1,2,3,4,5,6,7,8,9,10
[node1][distribution][my-executor]INFO  reset: periodical
[node1][distribution][my-executor][work]INFO  I am owning tasks: 1,2,3,4,5,6,7,8,9,10
[node1]INFO  exiting (bye bye 1)
%A
`
	expected2 := `
%A
[node2][distribution][my-executor]INFO  reset: initialization
[node2][distribution][my-executor][work]INFO  I am owning tasks: 1,2,3,4,5,6,7,8,9,10
[node2][distribution]INFO  found a new node "node3"
[node2][distribution][my-executor]INFO  reset: distribution changed: found a new node "node3"
[node2][distribution][my-executor][work]INFO  I am owning tasks: 4,7,8,9,10
[node2][distribution]INFO  found a new node "node4"
[node2][distribution][my-executor]INFO  reset: distribution changed: found a new node "node4"
[node2][distribution][my-executor][work]INFO  I am owning tasks: 4,8,9,10
[node2][distribution]INFO  the node "node3" gone
[node2][distribution][my-executor]INFO  reset: distribution changed: the node "node3" gone
[node2][distribution][my-executor][work]INFO  I am owning tasks: 1,3,4,6,8,9,10
[node2][distribution]INFO  the node "node4" gone
[node2][distribution][my-executor]INFO  reset: distribution changed: the node "node4" gone
[node2][distribution][my-executor][work]INFO  I am owning tasks: 1,2,3,4,5,6,7,8,9,10
[node2]INFO  exiting (bye bye 2)
%A
`
	expected3 := `
%A
[node3][distribution][my-executor]INFO  reset: initialization
[node3][distribution][my-executor][work]INFO  I am owning tasks: 1,2,3,5,6
[node3][distribution]INFO  found a new node "node4"
[node3][distribution][my-executor]INFO  reset: distribution changed: found a new node "node4"
[node3][distribution][my-executor][work]INFO  I am owning tasks: 1,3,6
[node3]INFO  exiting (bye bye 3)
%A
`
	expected4 := `
%A
[node4][distribution][my-executor]INFO  reset: initialization
[node4][distribution][my-executor][work]INFO  I am owning tasks: 2,5,7
[node4][distribution]INFO  the node "node3" gone
[node4][distribution][my-executor]INFO  reset: distribution changed: the node "node3" gone
[node4][distribution][my-executor][work]INFO  I am owning tasks: 2,5,7
[node4]INFO  exiting (bye bye 4)
%A
`
	wildcards.Assert(t, expected1, logsPerNode["node1"].String())
	wildcards.Assert(t, expected2, logsPerNode["node2"].String())
	wildcards.Assert(t, expected3, logsPerNode["node3"].String())
	wildcards.Assert(t, expected4, logsPerNode["node4"].String())
}

func createNodeWithExecutor(t *testing.T, clk clock.Clock, logs io.Writer, etcdNamespace string, nodeName string) (*Node, dependencies.Mocked) {
	t.Helper()
	node, d := createNode(t, clk, logs, etcdNamespace, nodeName)
	work := func(ctx context.Context, wg *sync.WaitGroup, logger log.Logger, assigner *Assigner) (initErr error) {
		logger = logger.AddPrefix("[work]")
		initDone := make(chan struct{})

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

			logger.Infof("I am owning tasks: %s", strings.Join(ownedTasks, ","))
			close(initDone)

			<-ctx.Done()
		}()

		// No init error
		<-initDone
		return nil
	}
	assert.NoError(t, node.StartExecutor("my-executor", work, WithResetInterval(resetInterval)))
	return node, d
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
