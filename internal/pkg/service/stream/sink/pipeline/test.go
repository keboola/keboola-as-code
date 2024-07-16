package pipeline

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
)

type TestOpener struct {
	ReopenOnSinkModification bool
	OpenError                error
	WriteRecordStatus        RecordStatus
	WriteError               error
	CloseError               error
}

type TestPipeline struct {
	opener *TestOpener
}

func NewTestOpener() *TestOpener {
	return &TestOpener{
		ReopenOnSinkModification: true,
		WriteRecordStatus:        RecordAccepted,
	}
}

func (o *TestOpener) OpenPipeline() (Pipeline, error) {
	if o.OpenError != nil {
		return nil, o.OpenError
	}
	return &TestPipeline{opener: o}, nil
}

func (p *TestPipeline) ReopenOnSinkModification() bool {
	return p.opener.ReopenOnSinkModification
}

func (p *TestPipeline) WriteRecord(_ recordctx.Context) (RecordStatus, error) {
	if err := p.opener.WriteError; err != nil {
		return RecordError, err
	}
	return p.opener.WriteRecordStatus, nil
}

func (p *TestPipeline) Close(_ context.Context) error {
	return p.opener.CloseError
}
