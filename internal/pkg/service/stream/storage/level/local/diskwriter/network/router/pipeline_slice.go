package router

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type SlicePipeline interface {
	IsReady() bool
	WriteRecord(c recordctx.Context) (pipeline.RecordStatus, error)
	Close(ctx context.Context) (err error)
}

type slicePipeline struct {
	router *Router
	slice  *sliceData

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	lock     sync.RWMutex
	pipeline encoding.Pipeline
}

func newSlicePipeline(ctx context.Context, ready *readyNotifier, router *Router, slice *sliceData) *slicePipeline {
	p := &slicePipeline{router: router, slice: slice}
	p.ctx, p.cancel = context.WithCancel(context.WithoutCancel(ctx))

	// Try to open pipeline in background, see IsReady method
	p.wg.Add(1)
	go func() {
		b := newOpenPipelineBackoff()
		defer p.wg.Done()
		for {
			// Try open pipeline
			if err := p.tryOpen(); err != nil {
				// Wait before retry
				delay := b.NextBackOff()
				p.router.logger.Warnf(p.ctx, "%s, waiting %s", err, delay)
				select {
				case <-time.After(delay):
					continue
				case <-p.ctx.Done():
					return
				}
			}

			// Pipeline opened, close goroutine
			ready.Ready()
			return
		}
	}()

	return p
}

// IsReady returns true if the pipeline and underlying network connection is healthy.
func (p *slicePipeline) IsReady() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.pipeline != nil && p.pipeline.IsReady()
}

func (p *slicePipeline) WriteRecord(c recordctx.Context) (pipeline.RecordStatus, error) {
	p.lock.RLock()
	defer p.lock.RUnlock()

	// pipeline is not nil, balancedPipeline checks IsReady method
	if err := p.pipeline.WriteRecord(c); err != nil {
		return pipeline.RecordError, err
	}

	// Record has been stored to OS disk cache or physical disk
	if p.slice.Encoding.Sync.Wait {
		return pipeline.RecordProcessed, nil
	}

	// Record has been stored in a in-memory buffer
	return pipeline.RecordAccepted, nil
}

func (p *slicePipeline) Close(ctx context.Context) (err error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Closed pipeline, if opened
	if p.pipeline != nil {
		err = p.pipeline.Close(ctx)
		if err != nil {
			p.router.logger.Errorf(ctx, "cannot close encoding pipeline for the slice %q: %s", p.slice.SliceKey, err)
		} else {
			p.router.logger.Infof(ctx, "closed encoding pipeline for the slice %q", p.slice.SliceKey)
		}
	}

	// Cancel open loop, if running
	p.cancel()
	p.wg.Wait()

	return err
}

func (p *slicePipeline) tryOpen() error {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Get connection
	conn, found := p.router.connections.ConnectionToVolume(p.slice.SliceKey.VolumeID)
	if !found || !conn.IsConnected() {
		return errors.Errorf("no connection to the volume %q for the slice %q", p.slice.SliceKey.VolumeID.String(), p.slice.SliceKey.String())
	}

	// Open remote RPC file
	remoteFile, err := rpc.OpenNetworkFile(p.ctx, p.router.nodeID, conn, p.slice.SliceKey)
	if err != nil {
		return errors.PrefixErrorf(err, "cannot open network file for the slice %q", p.slice.SliceKey.String())
	}

	// Open pipeline
	p.pipeline, err = p.router.encoding.OpenPipeline(p.ctx, p.slice.SliceKey, p.slice.Mapping, p.slice.Encoding, remoteFile)
	if err != nil {
		_ = remoteFile.Close(p.ctx)
		return errors.PrefixErrorf(err, "cannot open encoding pipeline for the slice %q", p.slice.SliceKey.String())
	}

	p.router.logger.Infof(p.ctx, "opened encoding pipeline for the slice %q", p.slice.SliceKey)
	return nil
}

func newOpenPipelineBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.1
	b.Multiplier = 4
	b.InitialInterval = 100 * time.Millisecond
	b.MaxInterval = 60 * time.Second
	b.MaxElapsedTime = 0 // don't stop
	b.Reset()
	return b
}
