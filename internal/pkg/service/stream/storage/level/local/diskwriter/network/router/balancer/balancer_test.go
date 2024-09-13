package balancer_test

import (
	"context"
	"fmt"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/balancer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

type TestPipeline struct {
	logger     io.Writer
	sliceKey   model.SliceKey
	Name       string
	Ready      bool
	WriteError error
	CloseError error
}

func NewTestPipeline(name string, sliceKey model.SliceKey, logger io.Writer) *TestPipeline {
	return &TestPipeline{
		Name:     name,
		sliceKey: sliceKey,
		logger:   logger,
		Ready:    true,
	}
}

func (p *TestPipeline) SliceKey() model.SliceKey {
	return p.sliceKey
}

func (p *TestPipeline) WriteRecord(c recordctx.Context) (pipeline.WriteResult, error) {
	if !p.Ready {
		return pipeline.WriteResult{Status: pipeline.RecordError}, balancer.PipelineNotReadyError{}
	}

	_, _ = fmt.Fprintf(p.logger, "write %s\n", p.Name)
	if p.WriteError != nil {
		return pipeline.WriteResult{Status: pipeline.RecordError}, p.WriteError
	}
	return pipeline.WriteResult{Status: pipeline.RecordProcessed}, nil
}

func (p *TestPipeline) Close(_ context.Context) error {
	_, _ = fmt.Fprintf(p.logger, "close %s\n", p.Name)
	return p.CloseError
}
