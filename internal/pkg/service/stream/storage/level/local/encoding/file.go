package encoding

import (
	"context"
	"io"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writechain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
)

// OutputOpener opens the network output for writing.
type OutputOpener interface {
	OpenOutput(sliceKey model.SliceKey) (writechain.File, error)
}

func OutputTo(w io.Writer) OutputOpener {
	return &writerOutputOpener{Writer: w}
}

type writerOutputOpener struct {
	io.Writer
}

func (o writerOutputOpener) OpenOutput(_ model.SliceKey) (writechain.File, error) {
	return &writerOutput{Writer: o.Writer}, nil
}

type writerOutput struct {
	io.Writer
}

func (*writerOutput) Sync() error {
	return nil // nop
}

func (*writerOutput) Close(context.Context) error {
	return nil // nop
}
