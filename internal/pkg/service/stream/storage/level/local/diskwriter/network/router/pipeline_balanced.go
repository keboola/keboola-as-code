package router

import (
	"context"
	"sync"

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

func newBalancedPipeline(ctx context.Context, router *Router, sinkKey key.SinkKey) (pipeline.Pipeline, error) {
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

	balanced := &balancedPipeline{}
	for _, sliceKey := range assigned {
		// sub.startOpenLoop()
		balanced.pipelines = append(balanced.pipelines, newSlicePipeline(ctx, router, sinkSlices[sliceKey]))
	}

	// return balanced, nil
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
	for _, p := range p.pipelines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.Close(ctx)
		}()
	}
	wg.Wait()

	p.pipelines = nil
	return nil
}
