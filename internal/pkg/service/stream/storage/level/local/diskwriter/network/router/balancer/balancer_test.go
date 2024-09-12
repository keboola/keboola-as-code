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

func (p *TestPipeline) WriteRecord(c recordctx.Context) (pipeline.RecordStatus, int, error) {
	if !p.Ready {
		return pipeline.RecordError, 0, balancer.PipelineNotReadyError{}
	}

	_, _ = fmt.Fprintf(p.logger, "write %s\n", p.Name)
	if p.WriteError != nil {
		return pipeline.RecordError, 0, p.WriteError
	}
	return pipeline.RecordProcessed, 0, nil
}

func (p *TestPipeline) Close(_ context.Context) error {
	_, _ = fmt.Fprintf(p.logger, "close %s\n", p.Name)
	return p.CloseError
}
