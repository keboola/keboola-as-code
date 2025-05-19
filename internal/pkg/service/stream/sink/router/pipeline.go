package router

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v5"

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
	onClose  func(ctx context.Context, cause string)

	// lock protects pipeline field
	lock sync.RWMutex
	// pipeline to write data to the sink,
	// it is initialized when the first record is received.
	pipeline pipeline.Pipeline

	openError      error
	openBackoff    backoff.BackOff
	openRetryAfter time.Time
}

func newPipelineRef(sink *sinkData, logger log.Logger, wg *sync.WaitGroup, plugins *plugin.Plugins, onClose func(ctx context.Context, cause string)) *pipelineRef {
	return &pipelineRef{
		logger:   logger.With(sink.sinkKey.Telemetry()...),
		sinkKey:  sink.sinkKey,
		sinkType: sink.sinkType,
		wg:       wg,
		plugins:  plugins,
		onClose:  onClose,
	}
}

func (r *pipelineRef) writeRecord(c recordctx.Context) (pipeline.WriteResult, error) {
	if err := r.ensureOpened(c); err != nil {
		return pipeline.WriteResult{Status: pipeline.RecordError}, err
	}
	return r.pipeline.WriteRecord(c)
}

func (r *pipelineRef) ensureOpened(c recordctx.Context) error {
	// Fast check
	r.lock.RLock()
	opened := r.pipeline != nil
	r.lock.RUnlock()
	if opened {
		return nil
	}

	// Try open, if needed, and there is no retry backoff delay active
	r.lock.Lock()
	defer r.lock.Unlock()

	// Add telemetry attributes
	ctx := ctxattr.ContextWith(c.Ctx(), r.sinkKey.Telemetry()...)

	if r.pipeline == nil && (r.openError == nil || c.Timestamp().After(r.openRetryAfter)) {
		var err error

		// Use plugin system to create the pipeline
		r.logger.Infof(ctx, `opening sink pipeline`)
		r.pipeline, err = r.plugins.OpenSinkPipeline(ctx, r.sinkKey, r.sinkType, r.unregister)

		// Use retry backoff, don't try to open pipeline on each record
		if err != nil {
			if r.openBackoff == nil {
				r.openBackoff = newOpenPipelineBackoff()
			}
			delay := r.openBackoff.NextBackOff()
			r.openRetryAfter = c.Timestamp().Add(delay)
			r.openError = errors.Errorf("cannot open sink pipeline: %w, next attempt after %s", err, utctime.From(r.openRetryAfter).String())
		} else {
			r.openError = nil
			r.openBackoff = nil
			r.openRetryAfter = time.Time{}
			r.logger.Infof(ctx, `opened sink pipeline`)
		}
	}

	if r.openError != nil {
		return r.openError
	}

	return nil
}

func (r *pipelineRef) unregister(ctx context.Context, cause string) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.pipeline == nil {
		return
	}

	r.logger.Infof(ctx, `closed sink pipeline: %s`, cause)

	r.onClose(ctx, cause)

	r.pipeline = nil
}

func (r *pipelineRef) close(ctx context.Context, cause string) {
	r.lock.Lock()
	p := r.pipeline
	r.lock.Unlock()
	if p != nil {
		r.logger.Debugf(ctx, "closing sink pipeline: %s", cause)
		p.Close(ctx, cause)
	}
}

func newOpenPipelineBackoff() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	b.RandomizationFactor = 0.2
	b.Multiplier = 2
	b.InitialInterval = 100 * time.Millisecond
	b.MaxInterval = 60 * time.Second
	b.Reset()
	return b
}
