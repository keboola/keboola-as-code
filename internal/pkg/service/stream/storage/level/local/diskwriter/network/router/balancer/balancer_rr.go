package balancer

import (
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// RoundRobinBalancer starts with a random pipeline index, from the start index, a ready pipeline is searched.
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
		status, err := pipelines[0].WriteRecord(c)
		if errors.As(err, &PipelineNotReadyError{}) {
			return pipeline.RecordError, NoPipelineReadyError{}
		}
		return status, err
	}

	start := int(b.counter.Add(1))
	for i := range length {
		index := (start + i) % length
		status, err := pipelines[index].WriteRecord(c)
		if errors.As(err, &PipelineNotReadyError{}) {
			// Pipeline is not ready, try next
			continue
		}
		return status, err
	}

	return pipeline.RecordError, NoPipelineReadyError{}
}
