package balancer

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Balancer selects from multiple SlicePipeline instances a one and writes to .
// If SlicePipeline.IsReady() == false, the pipeline is ignored.
// If no pipeline is ready, the NoPipelineReadyError is returned.
// The implementation must provide high throughput.
type Balancer interface {
	WriteRecord(c recordctx.Context, pipelines []SlicePipeline) (pipeline.RecordStatus, error)
}

type SlicePipeline interface {
	// WriteRecord method may return PipelineNotReadyError, then next pipeline will be tried.
	WriteRecord(c recordctx.Context) (pipeline.RecordStatus, error)
}

func NewBalancer(pipelineBalancer network.BalancerType) (Balancer, error) {
	switch pipelineBalancer {
	case network.RandomBalancerType:
		return NewRandomBalancer(), nil

	case network.RoundRobinBalancerType:
		return NewRoundRobinBalancer(), nil

	default:
		return nil, errors.New("invalid balancer selected")
	}
}
