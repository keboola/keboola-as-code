package router

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

type BalancerType string

const (
	randomBalancerType     BalancerType = "rand"
	roundRobinBalancerType BalancerType = "roundRobin"
)

// Balancer selects and writes to a slice pipeline.
// If SlicePipeline.IsReady() == false, the pipeline should be ignored.
// If no pipeline is ready, the NoPipelineReadyError should be returned.
type Balancer interface {
	WriteRecord(c recordctx.Context, pipelines []SlicePipeline) (pipeline.RecordStatus, error)
}

func NewBalancer(pipelineBalancer BalancerType) (Balancer, error) {
	switch pipelineBalancer {
	case randomBalancerType:
		return NewRandomBalancer(), nil

	case roundRobinBalancerType:
		return NewRoundRobinBalancer(), nil

	default:
	}

	return nil, errors.New("inavlid balancer selected")
}
