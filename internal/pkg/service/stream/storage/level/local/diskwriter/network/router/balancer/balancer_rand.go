package balancer

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// RandomBalancer starts with a random pipeline index, from the start index, a ready pipeline is searched.
type RandomBalancer struct {
	rand Randomizer
}

func NewRandomBalancer() Balancer {
	return NewRandomBalancerWithRandomizer(NewDefaultRandomizer())
}

func NewRandomBalancerWithRandomizer(r Randomizer) Balancer {
	return &RandomBalancer{rand: r}
}

func (b RandomBalancer) WriteRecord(c recordctx.Context, pipelines []SlicePipeline) (pipeline.RecordStatus, int, error) {
	length := len(pipelines)

	if length == 0 {
		return pipeline.RecordError, 0, NoPipelineError{}
	}

	if length == 1 {
		status, n, err := pipelines[0].WriteRecord(c)
		if errors.As(err, &PipelineNotReadyError{}) {
			return pipeline.RecordError, n, NoPipelineReadyError{}
		}
		return status, n, err
	}

	start := b.rand.IntN(length)
	for i := range length {
		index := (start + i) % length
		status, n, err := pipelines[index].WriteRecord(c)
		if errors.As(err, &PipelineNotReadyError{}) {
			// Pipeline is not ready, try next
			continue
		}
		return status, n, err
	}

	return pipeline.RecordError, 0, NoPipelineReadyError{}
}
