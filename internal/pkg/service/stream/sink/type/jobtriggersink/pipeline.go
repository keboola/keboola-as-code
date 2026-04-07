package jobtriggersink

import (
	"context"
	"strings"
	"sync/atomic"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	jsonnetWrapper "github.com/keboola/keboola-as-code/internal/pkg/encoding/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/jsonnet"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// SinkLoader loads a sink definition by its key.
// In production this wraps DefinitionRepository.Sink().Get(k).Do(ctx).ResultOrErr().
type SinkLoader func(ctx context.Context, k key.SinkKey) (definition.Sink, error)

// Pipeline implements pipeline.Pipeline for the jobTrigger sink type.
// Each WriteRecord call triggers a Keboola Queue job via the Queue API.
// Stats (triggered/failed counts) are accumulated in memory and flushed to etcd on Close.
type Pipeline struct {
	logger      log.Logger
	sinkKey     key.SinkKey
	sink        *definition.JobTriggerSink
	api         *keboola.AuthorizedAPI
	bridge      *Bridge
	jsonnetPool *jsonnetWrapper.VMPool[recordctx.Context]
	onClose     func(ctx context.Context, cause string)

	// In-memory stats accumulated across WriteRecord calls, flushed on Close.
	triggered        atomic.Uint64
	failed           atomic.Uint64
	firstTriggeredAt atomic.Pointer[utctime.UTCTime]
	lastTriggeredAt  atomic.Pointer[utctime.UTCTime]
}

// ReopenOnSinkModification returns true so the pipeline is recreated when the sink definition changes.
// This ensures updated componentId/configId/branchId take effect immediately.
func (p *Pipeline) ReopenOnSinkModification() bool {
	return true
}

// WriteRecord triggers a Keboola Queue job for the incoming record.
// If ConfigDataTemplate is set, it is evaluated as Jsonnet against the request context,
// and the result (a JSON object) is passed as configData to the job.
func (p *Pipeline) WriteRecord(c recordctx.Context) (pipeline.WriteResult, error) {
	req := p.api.NewCreateJobRequest(p.sink.ComponentID).
		WithBranch(p.sink.BranchID).
		WithConfig(p.sink.ConfigID)

	if p.sink.ConfigDataTemplate != "" {
		vm := p.jsonnetPool.Get()
		result, err := jsonnet.Evaluate(vm, c, p.sink.ConfigDataTemplate)
		p.jsonnetPool.Put(vm)
		if err != nil {
			p.failed.Add(1)
			return pipeline.WriteResult{Status: pipeline.RecordError},
				errors.Errorf("job trigger configDataTemplate evaluation failed: %w", err)
		}
		result = strings.TrimSpace(result)
		var configData map[string]any
		if err := json.DecodeString(result, &configData); err != nil {
			p.failed.Add(1)
			return pipeline.WriteResult{Status: pipeline.RecordError},
				errors.Errorf("job trigger configDataTemplate must produce a JSON object, got %q: %w", result, err)
		}
		req = req.WithConfigData(configData)
	}

	job, err := req.Build().Send(c.Ctx())
	if err != nil {
		p.failed.Add(1)
		return pipeline.WriteResult{Status: pipeline.RecordError},
			errors.Errorf("job trigger failed for %s/%s: %w", p.sink.ComponentID, p.sink.ConfigID, err)
	}

	now := utctime.From(c.Timestamp())
	p.triggered.Add(1)
	p.firstTriggeredAt.CompareAndSwap(nil, &now)
	p.lastTriggeredAt.Store(&now)

	p.logger.Infof(c.Ctx(), "triggered job %s for component %s config %s", job.ID, p.sink.ComponentID, p.sink.ConfigID)
	return pipeline.WriteResult{Status: pipeline.RecordProcessed}, nil
}

// Close flushes accumulated stats to etcd and invokes the onClose callback.
func (p *Pipeline) Close(ctx context.Context, cause string) {
	triggered := p.triggered.Load()
	failed := p.failed.Load()

	if triggered > 0 || failed > 0 {
		firstPtr := p.firstTriggeredAt.Load()
		lastPtr := p.lastTriggeredAt.Load()
		var firstAt, lastAt utctime.UTCTime
		if firstPtr != nil {
			firstAt = *firstPtr
		}
		if lastPtr != nil {
			lastAt = *lastPtr
		}
		if err := p.bridge.AddStats(ctx, p.sinkKey, triggered, failed, firstAt, lastAt); err != nil {
			p.logger.Errorf(ctx, "failed to flush job trigger stats for sink %s: %s", p.sinkKey, err)
		}
	}

	p.onClose(ctx, cause)
}

// NewOpener returns a pipeline.Opener for the SinkTypeJobTrigger sink type.
// The opener loads the sink definition via sinkLoader and builds an AuthorizedAPI
// using the stored token retrieved from the bridge.
func NewOpener(logger log.Logger, bridge *Bridge, sinkLoader SinkLoader) pipeline.Opener {
	return func(ctx context.Context, sinkKey key.SinkKey, sinkType definition.SinkType, onClose func(ctx context.Context, cause string)) (pipeline.Pipeline, error) {
		if sinkType != definition.SinkTypeJobTrigger {
			return nil, pipeline.NoOpenerFoundError{SinkType: sinkType}
		}

		// Load the full sink definition to get JobTriggerSink config.
		sink, err := sinkLoader(ctx, sinkKey)
		if err != nil {
			return nil, errors.Errorf("cannot load job trigger sink definition for %s: %w", sinkKey, err)
		}
		if sink.JobTrigger == nil {
			return nil, errors.Errorf("sink %s has type %q but JobTrigger config is nil", sinkKey, sinkType)
		}

		// Get project API using the stored token.
		api, err := bridge.APIForSink(ctx, sinkKey)
		if err != nil {
			return nil, err
		}

		return &Pipeline{
			logger:      logger,
			sinkKey:     sinkKey,
			sink:        sink.JobTrigger,
			api:         api,
			bridge:      bridge,
			jsonnetPool: jsonnet.NewPool(),
			onClose:     onClose,
		}, nil
	}
}
