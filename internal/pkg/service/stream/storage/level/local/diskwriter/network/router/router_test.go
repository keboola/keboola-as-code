package router_test

import (
	"context"
	"fmt"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
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

func (p *TestPipeline) IsReady() bool {
	return p.Ready
}

func (p *TestPipeline) WriteRecord(c recordctx.Context) (pipeline.RecordStatus, error) {
	_, _ = fmt.Fprintf(p.logger, "write %s\n", p.Name)
	if p.WriteError != nil {
		return pipeline.RecordError, p.WriteError
	}
	return pipeline.RecordProcessed, nil
}

func (p *TestPipeline) Close(_ context.Context) error {
	_, _ = fmt.Fprintf(p.logger, "close %s\n", p.Name)
	return p.CloseError
}
