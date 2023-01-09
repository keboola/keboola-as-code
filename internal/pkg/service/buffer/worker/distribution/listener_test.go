package distribution_test

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/keboola/go-utils/pkg/wildcards"
	gonanoid "github.com/matoous/go-nanoid/v2"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	. "github.com/keboola/keboola-as-code/internal/pkg/service/buffer/worker/distribution"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestOnChangeListener(t *testing.T) {
	t.Parallel()

	clk := clock.NewMock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var node1 *Node
	var d1, d2, d3, d4 dependencies.Mocked

	etcdNamespace := "unit-" + t.Name() + "-" + gonanoid.Must(8)
	client := etcdhelper.ClientForTestWithNamespace(t, etcdNamespace)

	// Create node with a listener
	node1, d1 = createNode(t, ctx, clk, etcdNamespace, "node1")
	listenerLogger := log.NewDebugLogger()
	listener := node1.OnChangeListener()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case events := <-listener.C:
				for _, event := range events {
					listenerLogger.Infof(`[listener] distribution changed: %s`, event.Message)
				}
			}
		}
	}()

	// Add node 2,3, stop node 2
	etcdhelper.ExpectModification(t, client, func() {
		_, d2 = createNode(t, ctx, clk, etcdNamespace, "node2")
		clk.Add(eventsGroupInterval)

		_, d3 = createNode(t, ctx, clk, etcdNamespace, "node3")
		clk.Add(eventsGroupInterval)

		d2.Process().Shutdown(errors.New("test"))
		d2.Process().WaitForShutdown()
		clk.Add(eventsGroupInterval)
	})

	// Stop listener
	listener.Stop()

	// Add node 4 (listener is stopped, no log msg expected)
	_, d4 = createNode(t, ctx, clk, etcdNamespace, "node4")

	// Stop all nodes (listener is stopped, no log msg expected)
	d1.Process().Shutdown(errors.New("test"))
	d1.Process().WaitForShutdown()
	d3.Process().Shutdown(errors.New("test"))
	d3.Process().WaitForShutdown()
	d4.Process().Shutdown(errors.New("test"))
	d4.Process().WaitForShutdown()

	// Check logs
	wildcards.Assert(t, `
INFO  [listener] distribution changed: found a new node "node1"
INFO  [listener] distribution changed: found a new node "node2"
INFO  [listener] distribution changed: found a new node "node3"
INFO  [listener] distribution changed: the node "node2" gone
`, listenerLogger.AllMessages())
}
