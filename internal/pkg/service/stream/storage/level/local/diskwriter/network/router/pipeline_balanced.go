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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type balancedPipeline struct {
	sinkKey   key.SinkKey
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
	p := &balancedPipeline{sinkKey: sinkKey, router: router}
	if err := p.openCloseSlices(ctx, true); err != nil {
		return nil, err
	}
	return p, nil
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

	p.router.pipelinesLock.Lock()
	delete(p.router.pipelines, p.sinkKey)
	p.router.pipelinesLock.Unlock()

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

	p.router.logger.Debugf(ctx, `closed balanced pipeline to %d slices, sink %q`, len(p.pipelines), p.sinkKey)
	p.pipelines = nil
	return nil
}

func (p *balancedPipeline) openCloseSlices(ctx context.Context, isNew bool) error {
	// Assign part of all sink slices to the balanced pipeline.
	// For each assigned slice, a sub pipeline is opened in the code bellow.
	sinkSlices := p.router.sinkOpenedSlices(p.sinkKey)

	// When opening the pipeline, at least one sink slice must exist
	if isNew && len(sinkSlices) == 0 {
		return NoOpenedSliceFoundError{}
	}

	assigned := assignment.AssignSlices(
		maps.Keys(sinkSlices),
		p.router.distribution.Nodes(),
		p.router.distribution.NodeID(),
		p.router.config.MinSlicesPerSourceNode,
	)

	// Convert existing pipelines to a map
	p.lock.RLock()
	existing := make(map[model.SliceKey]SlicePipeline)
	for _, sub := range p.pipelines {
		existing[sub.SliceKey()] = sub
	}
	p.lock.RUnlock()

	// Open pipelines for new slices
	existingCount := 0
	openedCount := 0
	ready := newReadyNotifier()
	active := make(map[model.SliceKey]bool)
	var pipelines []SlicePipeline
	for _, sliceKey := range assigned {
		if sub, found := existing[sliceKey]; found {
			existingCount++
			active[sliceKey] = true
			pipelines = append(pipelines, sub)
		} else {
			openedCount++
			active[sliceKey] = true
			pipelines = append(pipelines, newSlicePipeline(ctx, ready, p.router, sinkSlices[sliceKey]))
		}
	}

	// Wait until at least one pipeline is ready
	if existingCount == 0 && openedCount > 0 {
		select {
		case <-ready.WaitCh():
		case <-time.After(3 * time.Second): // move to config
		}
	}

	// Swap pipelines
	var old []SlicePipeline
	p.lock.Lock()
	old = p.pipelines
	p.pipelines = pipelines
	p.lock.Unlock()

	// Close balanced pipeline, if there is no sub pipeline at all
	if openedCount+existingCount == 0 {
		return p.Close(ctx)
	}

	// Close old pipelines, replaced by new pipelines, if any
	closedCount := 0
	for _, sub := range old {
		if _, found := active[sub.SliceKey()]; !found {
			closedCount++
			_ = sub.Close(ctx) // error is logged
		}
	}

	// Log result
	if openedCount > 0 || closedCount > 0 {
		if isNew {
			p.router.logger.Debugf(ctx, `opened balanced pipeline to %d slices, sink %q`, openedCount, p.sinkKey)
		} else {
			p.router.logger.Debugf(ctx, `updated balanced pipeline, %d opened slices, %d closed slices, sink %q`, openedCount, closedCount, p.sinkKey)
		}
	}

	return nil
}
