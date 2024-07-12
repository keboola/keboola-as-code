package router

import (
	"context"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type pipelineRef struct {
	*router
	sinkKey key.SinkKey
	// lock protects pipeline field
	lock sync.Mutex
	// pipeline to write data to the sink,
	// it is initialized when the first record is received.
	pipeline pipeline.Pipeline

	openError      error
	openBackoff    backoff.BackOff
	openRetryAfter time.Time
}

func (p *pipelineRef) close(ctx context.Context, reason string) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// Stop if the pipeline was not successfully opened
	if p.pipeline == nil {
		return
	}

	p.logger.Infof(ctx, `closing sink pipeline %q: %s`, p.sinkKey, reason)

	if err := p.pipeline.Close(ctx); err != nil {
		err := errors.PrefixErrorf(err, "cannot close sink pipeline %q", p.sinkKey)
		p.logger.Error(ctx, err.Error())
		return
	}

	p.logger.Infof(ctx, `closed sink pipeline %q: %s`, p.sinkKey, reason)
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
