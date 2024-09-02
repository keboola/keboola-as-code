package router

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/plugin"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type pipelineRef struct {
	sinkKey  key.SinkKey
	sinkType definition.SinkType
	logger   log.Logger
	wg       *sync.WaitGroup
	plugins  *plugin.Plugins
	onClose  func(context.Context)

	// lock protects pipeline field
	lock sync.RWMutex
	// pipeline to write data to the sink,
	// it is initialized when the first record is received.
	pipeline pipeline.Pipeline

	openError      error
	openBackoff    backoff.BackOff
	openRetryAfter time.Time
}

func newPipelineRef(sink *sinkData, logger log.Logger, wg *sync.WaitGroup, plugins *plugin.Plugins, onClose func(context.Context)) *pipelineRef {
	return &pipelineRef{
		logger:   logger.With(sink.sinkKey.Telemetry()...),
		sinkKey:  sink.sinkKey,
		sinkType: sink.sinkType,
		wg:       wg,
		plugins:  plugins,
		onClose:  onClose,
	}
}

func (p *pipelineRef) writeRecord(c recordctx.Context) (pipeline.RecordStatus, error) {
	if err := p.ensureOpened(c.Ctx(), c.Timestamp()); err != nil {
		return pipeline.RecordError, err
	}
	return p.pipeline.WriteRecord(c)
}

func (p *pipelineRef) ensureOpened(ctx context.Context, timestamp time.Time) error {
	// Fast check
	p.lock.RLock()
	opened := p.pipeline != nil
	p.lock.RUnlock()
	if opened {
		return nil
	}

	// Try open, if needed, and there is no retry backoff delay active
	p.lock.Lock()
	defer p.lock.Unlock()

	// Add telemetry attributes
	ctx = ctxattr.ContextWith(ctx, p.sinkKey.Telemetry()...)

	if p.pipeline == nil && (p.openError == nil || timestamp.After(p.openRetryAfter)) {
		var err error

		// Use plugin system to create the pipeline
		p.logger.Infof(ctx, `opening sink pipeline`)
		p.pipeline, err = p.plugins.OpenSinkPipeline(ctx, p.sinkKey, p.sinkType, p.close)

		// Use retry backoff, don't try to open pipeline on each record
		if err != nil {
			if p.openBackoff == nil {
				p.openBackoff = newOpenPipelineBackoff()
			}
			delay := p.openBackoff.NextBackOff()
			p.openRetryAfter = timestamp.Add(delay)
			p.openError = errors.Errorf("cannot open sink pipeline: %w, next attempt after %s", err, utctime.From(p.openRetryAfter).String())
		} else {
			p.openError = nil
			p.openBackoff = nil
			p.openRetryAfter = time.Time{}
			p.logger.Infof(ctx, `opened sink pipeline`)
		}
	}

	if p.openError != nil {
		return p.openError
	}

	return nil
}

func (p *pipelineRef) close(ctx context.Context, cause string) {
	p.onClose(ctx)

	p.lock.Lock()
	defer p.lock.Unlock()

	// Stop if the pipeline was not successfully opened
	if p.pipeline == nil {
		return
	}

	// Close pipeline in background, but wait for it on shutdown
	p.logger.Infof(ctx, `closing sink pipeline: %s`, cause)
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := p.pipeline.Close(ctx, cause); err != nil {
			err := errors.PrefixErrorf(err, "cannot close sink pipeline")
			p.logger.Error(ctx, err.Error())
			return
		}

		p.logger.Infof(ctx, `closed sink pipeline: %s`, cause)
	}()
}

func newOpenPipelineBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.2
	b.Multiplier = 2
	b.InitialInterval = 100 * time.Millisecond
	b.MaxInterval = 60 * time.Second
	b.MaxElapsedTime = 0 // don't stop
	b.Reset()
	return b
}
