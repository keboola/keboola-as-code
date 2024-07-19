package router

import (
	"context"
	"sync"
	"time"

	"golang.org/x/exp/maps"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/assignment"
)

type balancedPipeline struct {
	router    *Router
	lock      sync.RWMutex
	pipelines []SlicePipeline
}

type readyNotifier struct {
	lock  sync.Mutex
	ready chan struct{}
}

func newReadyNotifier() *readyNotifier {
	return &readyNotifier{ready: make(chan struct{})}
}

func (n *readyNotifier) Ready() {
	n.lock.Lock()
	defer n.lock.Unlock()
	select {
	case <-n.ready:
	default:
		close(n.ready)
	}
}

func (n *readyNotifier) WaitCh() <-chan struct{} {
	return n.ready
}

func newBalancedPipeline(ctx context.Context, router *Router, sinkKey key.SinkKey) (*balancedPipeline, error) {
	sinkSlices, err := router.sinkOpenedSlices(sinkKey)
	if err != nil {
		return nil, err
	}

	assigned := assignment.AssignSlices(
		maps.Keys(sinkSlices),
		router.distribution.Nodes(),
		router.distribution.NodeID(),
		router.config.MinSlicesPerSourceNode,
	)

	ready := newReadyNotifier()
	balanced := &balancedPipeline{router: router}
	for _, sliceKey := range assigned {
		balanced.pipelines = append(balanced.pipelines, newSlicePipeline(ctx, ready, router, sinkSlices[sliceKey]))
	}

	select {
	case <-ready.WaitCh():
	case <-time.After(3 * time.Second): // move to config
	}

	return balanced, nil
}

func (p *balancedPipeline) ReopenOnSinkModification() bool {
	// We are watching for slices changes, we don't need other reopen mechanism.
	return false
}

func (p *balancedPipeline) WriteRecord(c recordctx.Context) (pipeline.RecordStatus, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.router.balancer.WriteRecord(c, p.pipelines)
}

func (p *balancedPipeline) Close(ctx context.Context) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	wg := &sync.WaitGroup{}
	for _, pipeline := range p.pipelines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := pipeline.Close(ctx); err != nil {
				p.router.logger.Errorf(ctx, "cannot close slice pipeline: %s", err)
			}
		}()
	}
	wg.Wait()

	p.pipelines = nil
	return nil
}
