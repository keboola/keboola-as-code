package diskwriter

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"os"
)

// Writer writes bytes to the slice file on the disk.
// Thw Writer.Write method is called by the network.Listener, which received bytes from the encoding.Writer.
type Writer interface {
	SliceKey() model.SliceKey
	Write(p []byte) (n int, err error)
	// Sync data from OS cache to the disk.
	Sync() error
	// Close the writer and sync data to the disk.
	Close(context.Context) error
	// Events provides listening to the writer lifecycle.
	Events() *events.Events[Writer]
}

type writer struct {
	slice  *model.Slice
	file   *os.File
	events *events.Events[Writer]
}

func New(
	ctx context.Context,
	logger log.Logger,
	slice *model.Slice,
	file *os.File,
	writerEvents *events.Events[Writer],
) (out Writer, err error) {
	return &writer{
		slice:  slice,
		file:   file,
		events: writerEvents.Clone(),
	}, nil
}

func (w *writer) SliceKey() model.SliceKey {
	return w.slice.SliceKey
}

func (w *writer) Write(p []byte) (n int, err error) {
	return w.file.Write(p)
}

func (w *writer) Sync() error {
	return w.file.Sync()
}

func (w *writer) Close(ctx context.Context) error {
	return w.file.Close()
}

func (w *writer) Events() *events.Events[Writer] {
	return w.events
}
