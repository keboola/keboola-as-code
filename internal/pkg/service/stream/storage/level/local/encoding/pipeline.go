package encoding

import (
	"context"
	"io"
	"sync"

	"github.com/benbjohnson/clock"
	"github.com/c2h5oh/datasize"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	compressionWriter "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression/writer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/count"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/encoder"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/limitbuffer"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/size"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writechain"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/events"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Pipeline processes record values using the configured Encoder and compression.
type Pipeline interface {
	StatisticsProvider

	SliceKey() model.SliceKey

	WriteRecord(record recordctx.Context) error
	// Events provides listening to the writer lifecycle.
	Events() *events.Events[Pipeline]
	// Close the writer and sync data to the disk.
	Close(context.Context) error
}

type StatisticsProvider interface {
	// AcceptedWrites returns count of write operations waiting for the sync.
	AcceptedWrites() uint64
	// CompletedWrites returns count of successfully written and synced writes.
	CompletedWrites() uint64
	// FirstRecordAt returns timestamp of receiving the first record for processing.
	FirstRecordAt() utctime.UTCTime
	// LastRecordAt returns timestamp of receiving the last record for processing.
	LastRecordAt() utctime.UTCTime
	// CompressedSize written to the file, measured after compression writer.
	CompressedSize() datasize.ByteSize
	// UncompressedSize written to the file, measured before compression writer.
	UncompressedSize() datasize.ByteSize
}

// pipeline implements Pipeline interface, it wraps common logic for all file types.
// For conversion between record values and bytes, the encoder.Encoder is used.
type pipeline struct {
	logger   log.Logger
	sliceKey model.SliceKey
	events   *events.Events[Pipeline]

	encoder encoder.Encoder
	chain   *writechain.Chain
	syncer  *writesync.Syncer

	// closed blocks new writes
	closed chan struct{}
	// writeWg waits for in-progress writes before Close
	writeWg *sync.WaitGroup

	acceptedWrites    *count.Counter
	completedWrites   *count.Counter
	compressedMeter   *size.Meter
	uncompressedMeter *size.Meter
}

func newPipeline(
	ctx context.Context,
	logger log.Logger,
	clk clock.Clock,
	sliceKey model.SliceKey,
	cfg config.Config,
	mapping table.Mapping,
	output writechain.File,
	events *events.Events[Pipeline],
) (out Pipeline, err error) {
	w := &pipeline{
		logger:   logger.WithComponent("slice-writer"),
		sliceKey: sliceKey,
		events:   events.Clone(), // clone passed events, so additional pipeline specific listeners can be added
		closed:   make(chan struct{}),
		writeWg:  &sync.WaitGroup{},
	}

	ctx = ctxattr.ContextWith(ctx, attribute.String("slice", sliceKey.String()))
	w.logger.Debug(ctx, "opening encoding pipeline")

	// Close resources on error
	defer func() {
		if err != nil {
			if w.syncer != nil {
				_ = w.syncer.Stop(ctx)
			}
			if w.chain != nil {
				_ = w.chain.Close(ctx)
			}
		}
	}()

	// Create counters
	{
		// In progress writes counter
		w.acceptedWrites = count.NewCounter()

		// Successful writes counter, the value backup is periodically saved to disk.
		// In the case of non-graceful node shutdown, the initial state is loaded from the disk after the node is restarted.
		// In that case, the statistics may not be accurate, but this should not happen, and we prefer throughput over the atomicity of statistics.
		w.completedWrites = count.NewCounter()
	}

	// Create empty chain of writers to the file
	// The chain is built from the end, from the output file, to the CSV writer at the start
	{
		w.chain = writechain.New(logger, output)
	}

	// Add a buffer before the file
	{
		if cfg.OutputBuffer > 0 {
			w.chain.PrependWriter(func(w io.Writer) io.Writer {
				return limitbuffer.New(w, int(cfg.OutputBuffer.Bytes()))
			})
		}
	}

	// Measure size of compressed data
	{
		w.chain.PrependWriter(func(writer io.Writer) io.Writer {
			w.compressedMeter = size.NewMeter(writer)
			return w.compressedMeter
		})
	}

	// Add compression if enabled
	{
		if cfg.Compression.Type != compression.TypeNone {
			// Add compression writer
			_, err = w.chain.PrependWriterOrErr(func(writer io.Writer) (io.Writer, error) {
				return compressionWriter.New(writer, cfg.Compression)
			})
			if err != nil {
				return nil, err
			}

			// Add a buffer before compression writer, if it is not included in the writer itself
			if cfg.InputBuffer > 0 && !cfg.Compression.HasWriterInputBuffer() {
				w.chain.PrependWriter(func(w io.Writer) io.Writer {
					return limitbuffer.New(w, int(cfg.InputBuffer.Bytes()))
				})
			}

			// Measure size of uncompressed CSV data
			w.chain.PrependWriter(func(writer io.Writer) io.Writer {
				w.uncompressedMeter = size.NewMeter(writer)
				return w.uncompressedMeter
			})
		} else {
			// Size of the compressed and uncompressed data is same
			w.uncompressedMeter = w.compressedMeter
		}
	}

	// Create syncer to trigger sync based on counter and meters from the previous steps
	{
		var syncerFactory writesync.SyncerFactory = writesync.DefaultSyncerFactory{}
		if cfg.Sync.OverrideSyncerFactory != nil {
			syncerFactory = cfg.Sync.OverrideSyncerFactory
		}
		w.syncer = syncerFactory.NewSyncer(ctx, logger, clk, cfg.Sync, w.chain, w)
	}

	// Create file format writer.
	// It is entrypoint of the writers chain.
	{
		w.encoder, err = cfg.Encoder.Factory.NewEncoder(cfg.Encoder, mapping, w.chain)
		if err != nil {
			return nil, err
		}

		// Flush/Close the file format writer at first
		w.chain.PrependFlusherCloser(w.encoder)
	}

	// Dispatch "open" event
	if err = w.events.DispatchOnOpen(w); err != nil {
		return nil, err
	}

	w.logger.Debug(ctx, "opened encoding pipeline")
	return w, nil
}

func (w *pipeline) WriteRecord(record recordctx.Context) error {
	timestamp := record.Timestamp()

	// Block Close method
	w.writeWg.Add(1)
	defer w.writeWg.Done()

	// Check if the writer is closed
	if w.isClosed() {
		return errors.New(`writer is closed`)
	}

	// Format and write table row
	if err := w.encoder.WriteRecord(record); err != nil {
		return err
	}

	notifier := w.syncer.Notifier()

	// Increments number of high-level writes in progress
	w.acceptedWrites.Add(timestamp, 1)

	// Wait for sync and return sync error, if any
	if err := notifier.Wait(); err != nil {
		return err
	}

	// Increase the count of successful writes
	w.completedWrites.Add(timestamp, 1)
	return nil
}

func (w *pipeline) SliceKey() model.SliceKey {
	return w.sliceKey
}

func (w *pipeline) Events() *events.Events[Pipeline] {
	return w.events
}

// AcceptedWrites returns count of write operations waiting for the sync.
func (w *pipeline) AcceptedWrites() uint64 {
	return w.acceptedWrites.Count()
}

// CompletedWrites returns count of successfully written and synced writes.
func (w *pipeline) CompletedWrites() uint64 {
	return w.completedWrites.Count()
}

// FirstRecordAt returns timestamp of receiving the first row for processing.
func (w *pipeline) FirstRecordAt() utctime.UTCTime {
	return w.completedWrites.FirstAt()
}

// LastRecordAt returns timestamp of receiving the last row for processing.
func (w *pipeline) LastRecordAt() utctime.UTCTime {
	return w.completedWrites.LastAt()
}

// CompressedSize written to the file, measured after compression writer.
func (w *pipeline) CompressedSize() datasize.ByteSize {
	return w.compressedMeter.Size()
}

// UncompressedSize written to the file, measured before compression writer.
func (w *pipeline) UncompressedSize() datasize.ByteSize {
	return w.uncompressedMeter.Size()
}

func (w *pipeline) Close(ctx context.Context) error {
	w.logger.Debug(ctx, "closing encoding pipeline")

	// Close only once
	if w.isClosed() {
		return errors.New(`writer is already closed`)
	}
	close(w.closed)

	errs := errors.NewMultiError()

	// Stop syncer, it triggers also the last sync.
	// Returns "syncer is already stopped" error, if called multiple times.
	if err := w.syncer.Stop(ctx); err != nil {
		errs.Append(err)
	}

	// Wait for running writes
	w.writeWg.Wait()

	// Close writers  chain, it closes all writers, and then sync/close the file.
	if err := w.chain.Close(ctx); err != nil {
		errs.Append(err)
	}

	// Dispatch "close"" event
	if err := w.events.DispatchOnClose(w, errs.ErrorOrNil()); err != nil {
		errs.Append(err)
	}

	w.logger.Debug(ctx, "closed encoding pipeline")
	return errs.ErrorOrNil()
}

func (w *pipeline) isClosed() bool {
	select {
	case <-w.closed:
		return true
	default:
		return false
	}
}
