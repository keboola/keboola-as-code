package pipeline

import (
	"context"
	"fmt"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
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

type NoOpenerFoundError struct {
	SinkType definition.SinkType
}

func (e NoOpenerFoundError) Error() string {
	return fmt.Sprintf("no pipeline opener found for the sink type %q", e.SinkType)
}

type Pipeline interface {
	WriteRecord(c recordctx.Context) (RecordStatus, error)
	Close(ctx context.Context) error
}

// Opener opens Pipeline for the sink.
// If the returned pipeline is nil, it means, the opener cannot handle the sink type,
// then the plugin system will try the next opener.
type Opener func(ctx context.Context, sink definition.Sink) (Pipeline, error)
