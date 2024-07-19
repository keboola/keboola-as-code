package router

import (
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
)

// Balancer selects and writes to a slice pipeline.
// If SlicePipeline.IsReady() == false, the pipeline should be ignored.
// If no pipeline is ready, the NoPipelineReadyError should be returned.
type Balancer interface {
	WriteRecord(c recordctx.Context, pipelines []SlicePipeline) (pipeline.RecordStatus, error)
}

type NoPipelineError struct{}

type NoPipelineReadyError struct{}

func (e NoPipelineError) Error() string {
	return "no pipeline"
}

func (e NoPipelineReadyError) Error() string {
	return "no pipeline is ready"
}