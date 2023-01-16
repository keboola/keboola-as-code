package distribution_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"

	bufferDependencies "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

func TestOnChangeListener(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var node1 *Node
	var d1, d2, d3, d4 bufferDependencies.Mocked

	listenerLogs := ioutil.NewBufferedWriter()
	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)

	// Create node with a listener
	node1, d1 = createNode(t, clk, nil, etcdNamespace, "node1")
	count := atomic.NewInt64(0)
	listener := node1.OnChangeListener()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case events := <-listener.C:
				for _, event := range events {
					_, _ = listenerLogs.WriteString(fmt.Sprintf("distribution changed: %s\n", event.Message))
					count.Inc()
				}
			}
		}
	}()

	// Add node 2,3, stop node 2
	_, d2 = createNode(t, clk, nil, etcdNamespace, "node2")
	_, d3 = createNode(t, clk, nil, etcdNamespace, "node3")
	d2.Process().Shutdown(errors.New("test"))
	d2.Process().WaitForShutdown()
	assert.Eventually(t, func() bool {
		return count.Load() == 3
	}, time.Second, 10*time.Millisecond, "timeout")

	// Stop listener
	listener.Stop()

	// Add node 4 (listener is stopped, no log msg expected)
	_, d4 = createNode(t, clk, nil, etcdNamespace, "node4")

	// Stop all nodes (listener is stopped, no log msg expected)
	d1.Process().Shutdown(errors.New("test"))
	d1.Process().WaitForShutdown()
	d3.Process().Shutdown(errors.New("test"))
	d3.Process().WaitForShutdown()
	d4.Process().Shutdown(errors.New("test"))
	d4.Process().WaitForShutdown()

	// Check logs
	wildcards.Assert(t, `
distribution changed: found a new node "node2"
distribution changed: found a new node "node3"
distribution changed: the node "node2" gone
`, listenerLogs.String())
}
