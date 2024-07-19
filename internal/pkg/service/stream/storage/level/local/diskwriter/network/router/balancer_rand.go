package router

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
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

func (b RandomBalancer) WriteRecord(c recordctx.Context, pipelines []SlicePipeline) (pipeline.RecordStatus, error) {
	length := len(pipelines)

	if length == 0 {
		return pipeline.RecordError, NoPipelineError{}
	}

	if length == 1 {
		if pipelines[0].IsReady() {
			return pipelines[0].WriteRecord(c)
		}
	}

	start := b.rand.IntN(length)
	for i := range length {
		index := (start + i) % length
		if p := pipelines[index]; p.IsReady() {
			return p.WriteRecord(c)
		}
	}

	return pipeline.RecordError, NoPipelineReadyError{}
}
