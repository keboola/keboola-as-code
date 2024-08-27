package router

import (
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
)

// RandomBalancer starts with a random pipeline index, from the start index, a ready pipeline is searched.
type RoundRobinBalancer struct {
	counter *atomic.Int64
}

func NewRoundRobinBalancer() Balancer {
	return &RoundRobinBalancer{counter: atomic.NewInt64(-1)}
}

func (b RoundRobinBalancer) WriteRecord(c recordctx.Context, pipelines []SlicePipeline) (pipeline.RecordStatus, error) {
	length := len(pipelines)

	if length == 0 {
		return pipeline.RecordError, NoPipelineError{}
	}

	if length == 1 {
		if pipelines[0].IsReady() {
			return pipelines[0].WriteRecord(c)
		}
	}

	start := int(b.counter.Add(1))
	for i := range length {
		index := (start + i) % length
		if p := pipelines[index]; p.IsReady() {
			return p.WriteRecord(c)
		}
	}

	return pipeline.RecordError, NoPipelineReadyError{}
}
