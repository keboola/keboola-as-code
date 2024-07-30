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
	sinkKey key.SinkKey
	router  *Router

	// updateLock protects open, update and close operations
	updateLock sync.Mutex

	// pipelinesLock protects reading and swapping of the sub pipelines
	pipelinesLock sync.RWMutex
	pipelines     []SlicePipeline
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

func openBalancedPipeline(ctx context.Context, router *Router, sinkKey key.SinkKey) (*balancedPipeline, error) {
	p := &balancedPipeline{sinkKey: sinkKey, router: router}
	if err := p.update(ctx, true); err != nil {
		return nil, err
	}
	router.registerPipeline(p)
	return p, nil
}

func (p *balancedPipeline) ReopenOnSinkModification() bool {
	// We are watching for slices changes, we don't need other reopen mechanism.
	return false
}

func (p *balancedPipeline) WriteRecord(c recordctx.Context) (pipeline.RecordStatus, error) {
	p.pipelinesLock.RLock()
	defer p.pipelinesLock.RUnlock()
	return p.router.balancer.WriteRecord(c, p.pipelines)
}

func (p *balancedPipeline) Close(ctx context.Context) error {
	p.updateLock.Lock()
	defer p.updateLock.Unlock()
	return p.close(ctx)
}

// update reacts on slices changes - closes old pipelines and open new pipelines.
func (p *balancedPipeline) update(ctx context.Context, isNew bool) error {
	p.updateLock.Lock()
	defer p.updateLock.Unlock()

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
	p.pipelinesLock.RLock()
	existing := make(map[model.SliceKey]SlicePipeline)
	for _, sub := range p.pipelines {
		existing[sub.SliceKey()] = sub
	}
	p.pipelinesLock.RUnlock()

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

	// Close balanced pipeline, if there is no sub pipeline at all
	if len(pipelines) == 0 {
		return p.close(ctx) // call close, but we already have the update lock
	}

	// Swap pipelines quickly, we do not block writes
	var old []SlicePipeline
	p.pipelinesLock.Lock()
	old = p.pipelines
	p.pipelines = pipelines
	p.pipelinesLock.Unlock()

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

func (p *balancedPipeline) close(ctx context.Context) error {
	p.pipelinesLock.Lock()
	defer p.pipelinesLock.Unlock()

	p.router.unregisterPipeline(p)

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
