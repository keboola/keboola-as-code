package distribution_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/keboola/go-utils/pkg/wildcards"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/ioutil"
)

func TestOnChangeListener(t *testing.T) {
	t.Parallel()

	clk := clockwork.NewFakeClock()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	var node1 *distribution.GroupNode
	var d1, d2, d3, d4 dependencies.Mocked

	listenerLogs := ioutil.NewAtomicWriter()
	etcdCfg := etcdhelper.TmpNamespace(t)

	// Create node with a listener
	node1, d1 = createNode(t, ctx, clk, etcdCfg, "node1")
	listener := node1.OnChangeListener()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case events := <-listener.C:
				for _, event := range events {
					_, _ = fmt.Fprintf(listenerLogs, "distribution changed: %s\n", event.Message)
				}
			}
		}
	}()

	// Add node 2
	_, d2 = createNode(t, ctx, clk, etcdCfg, "node2")
	assert.Eventually(t, func() bool {
		return strings.Contains(listenerLogs.String(), `found a new node "node2"`)
	}, 10*time.Second, 10*time.Millisecond, "timeout")

	// Add node 3
	_, d3 = createNode(t, ctx, clk, etcdCfg, "node3")
	assert.Eventually(t, func() bool {
		return strings.Contains(listenerLogs.String(), `found a new node "node3"`)
	}, 10*time.Second, 10*time.Millisecond, "timeout")

	// Stop node 2
	d2.Process().Shutdown(ctx, errors.New("test"))
	d2.Process().WaitForShutdown()
	assert.Eventually(t, func() bool {
		return strings.Contains(listenerLogs.String(), `the node "node2" gone`)
	}, 10*time.Second, 10*time.Millisecond, "timeout")

	// Stop listener
	listener.Stop()

	// Add node 4 (listener is stopped, no log msg expected)
	_, d4 = createNode(t, ctx, clk, etcdCfg, "node4")

	// Stop all nodes (listener is stopped, no log msg expected)
	d1.Process().Shutdown(ctx, errors.New("test"))
	d1.Process().WaitForShutdown()
	d3.Process().Shutdown(ctx, errors.New("test"))
	d3.Process().WaitForShutdown()
	d4.Process().Shutdown(ctx, errors.New("test"))
	d4.Process().WaitForShutdown()

	// Check logs
	wildcards.Assert(t, `
distribution changed: found a new node "node2"
distribution changed: found a new node "node3"
distribution changed: the node "node2" gone
`, listenerLogs.String())
}
