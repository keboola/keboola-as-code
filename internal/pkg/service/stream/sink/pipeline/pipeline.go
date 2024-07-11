package pipeline

import (
	"context"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
)

const (
	// RecordAccepted - the record has been successfully written to the pipeline without waiting for the result.
	RecordAccepted RecordStatus = iota
	// RecordProcessed - the record has been successfully written to a persistent storage or service.
	RecordProcessed
	// RecordError - write operation failed.
	RecordError
)

type RecordStatus int

type Pipeline interface {
	WriteRecord(c recordctx.Context) (RecordStatus, error)
	Close(ctx context.Context) error
}

// Opener opens Pipeline for the sink.
// If the returned pipeline is nil, it means, the opener cannot handle the sink type,
// then the plugin system will try the next opener.
type Opener func(ctx context.Context, sink definition.Sink) (Pipeline, error)
