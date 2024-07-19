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
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
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

	// IsReady is used for load balancing, to detect health pipelines.
	IsReady() bool
	// WriteRecord blocks until the record is written and synced to the local storage, if the wait is enabled.
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

// NetworkFile represent a file on a disk writer node, connected via network.
type NetworkFile interface {
	// IsReady returns true if the underlying network is working.
	IsReady() bool
	// Write bytes to the buffer in the disk writer node.
	Write(p []byte) (n int, err error)
	// Flush buffered bytes to the OS disk cache,
	// so only completed parts are passed to the disk.
	Flush(ctx context.Context) error
	// Sync OS disk cache to the physical disk.
	Sync(ctx context.Context) error
	// Close the underlying OS file and network connection.
	Close(ctx context.Context) error
}

// pipeline implements Pipeline interface, it wraps common logic for all file types.
// For conversion between record values and bytes, the encoder.Encoder is used.
type pipeline struct {
	logger   log.Logger
	sliceKey model.SliceKey
	events   *events.Events[Pipeline]

	encoder encoder.Encoder
	output  NetworkFile
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
	mappingCfg table.Mapping,
	encodingCfg encoding.Config,
	output NetworkFile,
	events *events.Events[Pipeline],
) (out Pipeline, err error) {
	p := &pipeline{
		logger:   logger.WithComponent("slice-writer"),
		sliceKey: sliceKey,
		output:   output,
		events:   events.Clone(), // clone passed events, so additional pipeline specific listeners can be added
		closed:   make(chan struct{}),
		writeWg:  &sync.WaitGroup{},
	}

	ctx = ctxattr.ContextWith(ctx, attribute.String("slice", sliceKey.String()))
	p.logger.Debug(ctx, "opening encoding pipeline")

	// Close resources on error
	defer func() {
		if err != nil {
			if p.syncer != nil {
				_ = p.syncer.Stop(ctx)
			}
			if p.chain != nil {
				_ = p.chain.Close(ctx)
			}
		}
	}()

	// Create counters
	{
		// In progress writes counter
		p.acceptedWrites = count.NewCounter()

		// Successful writes counter, the value backup is periodically saved to disk.
		// In the case of non-graceful node shutdown, the initial state is loaded from the disk after the node is restarted.
		// In that case, the statistics may not be accurate, but this should not happen, and we prefer throughput over the atomicity of statistics.
		p.completedWrites = count.NewCounter()
	}

	// Create empty chain of writers to the file
	// The chain is built from the end, from the output file, to the CSV writer at the start
	{
		p.chain = writechain.New(logger, p.output)
	}

	// Add a buffer before the file
	{
		if encodingCfg.OutputBuffer > 0 {
			p.chain.PrependWriter(func(w io.Writer) io.Writer {
				return limitbuffer.New(w, int(encodingCfg.OutputBuffer.Bytes()))
			})
		}
	}

	// Measure size of compressed data
	{
		p.chain.PrependWriter(func(writer io.Writer) io.Writer {
			p.compressedMeter = size.NewMeter(writer)
			return p.compressedMeter
		})
	}

	// Add compression if enabled
	{
		if encodingCfg.Compression.Type != compression.TypeNone {
			// Add compression writer
			_, err = p.chain.PrependWriterOrErr(func(writer io.Writer) (io.Writer, error) {
				return compressionWriter.New(writer, encodingCfg.Compression)
			})
			if err != nil {
				return nil, err
			}

			// Add a buffer before compression writer, if it is not included in the writer itself
			if encodingCfg.InputBuffer > 0 && !encodingCfg.Compression.HasWriterInputBuffer() {
				p.chain.PrependWriter(func(w io.Writer) io.Writer {
					return limitbuffer.New(w, int(encodingCfg.InputBuffer.Bytes()))
				})
			}

			// Measure size of uncompressed CSV data
			p.chain.PrependWriter(func(writer io.Writer) io.Writer {
				p.uncompressedMeter = size.NewMeter(writer)
				return p.uncompressedMeter
			})
		} else {
			// Size of the compressed and uncompressed data is same
			p.uncompressedMeter = p.compressedMeter
		}
	}

	// Create syncer to trigger sync based on counter and meters from the previous steps
	{
		var syncerFactory writesync.SyncerFactory = writesync.DefaultSyncerFactory{}
		if encodingCfg.Sync.OverrideSyncerFactory != nil {
			syncerFactory = encodingCfg.Sync.OverrideSyncerFactory
		}
		p.syncer = syncerFactory.NewSyncer(ctx, logger, clk, encodingCfg.Sync, p.chain, p)
	}

	// Create file format writer.
	// It is entrypoint of the writers chain.
	{
		// Get factory
		var encoderFactory encoder.Factory = encoder.DefaultFactory{}
		if encodingCfg.Encoder.OverrideEncoderFactory != nil {
			encoderFactory = encodingCfg.Encoder.OverrideEncoderFactory
		}

		// Create encoder
		p.encoder, err = encoderFactory.NewEncoder(encodingCfg.Encoder, mappingCfg, p.chain)
		if err != nil {
			return nil, err
		}

		// Flush/Close the file format writer at first
		p.chain.PrependFlusherCloser(p.encoder)
	}

	// Dispatch "open" event
	if err = p.events.DispatchOnOpen(p); err != nil {
		return nil, err
	}

	p.logger.Debug(ctx, "opened encoding pipeline")
	return p, nil
}

func (p *pipeline) IsReady() bool {
	return p.output.IsReady()
}

func (p *pipeline) WriteRecord(record recordctx.Context) error {
	timestamp := record.Timestamp()

	// Block Close method
	p.writeWg.Add(1)
	defer p.writeWg.Done()

	// Check if the writer is closed
	if p.isClosed() {
		return errors.New(`writer is closed`)
	}

	// Format and write table row
	if err := p.encoder.WriteRecord(record); err != nil {
		return err
	}

	notifier := p.syncer.Notifier()

	// Increments number of high-level writes in progress
	p.acceptedWrites.Add(timestamp, 1)

	// Wait for sync and return sync error, if any
	if err := notifier.Wait(); err != nil {
		return err
	}

	// Increase the count of successful writes
	p.completedWrites.Add(timestamp, 1)
	return nil
}

func (p *pipeline) SliceKey() model.SliceKey {
	return p.sliceKey
}

func (p *pipeline) Events() *events.Events[Pipeline] {
	return p.events
}

// AcceptedWrites returns count of write operations waiting for the sync.
func (p *pipeline) AcceptedWrites() uint64 {
	return p.acceptedWrites.Count()
}

// CompletedWrites returns count of successfully written and synced writes.
func (p *pipeline) CompletedWrites() uint64 {
	return p.completedWrites.Count()
}

// FirstRecordAt returns timestamp of receiving the first row for processing.
func (p *pipeline) FirstRecordAt() utctime.UTCTime {
	return p.completedWrites.FirstAt()
}

// LastRecordAt returns timestamp of receiving the last row for processing.
func (p *pipeline) LastRecordAt() utctime.UTCTime {
	return p.completedWrites.LastAt()
}

// CompressedSize written to the file, measured after compression writer.
func (p *pipeline) CompressedSize() datasize.ByteSize {
	return p.compressedMeter.Size()
}

// UncompressedSize written to the file, measured before compression writer.
func (p *pipeline) UncompressedSize() datasize.ByteSize {
	return p.uncompressedMeter.Size()
}

func (p *pipeline) Close(ctx context.Context) error {
	p.logger.Debug(ctx, "closing encoding pipeline")

	// Close only once
	if p.isClosed() {
		return nil
	}
	close(p.closed)

	errs := errors.NewMultiError()

	// Stop syncer, it triggers also the last sync.
	// Returns "syncer is already stopped" error, if called multiple times.
	if err := p.syncer.Stop(ctx); err != nil {
		errs.Append(err)
	}

	// Wait for running writes
	p.writeWg.Wait()

	// Close writers  chain, it closes all writers, and then sync/close the file.
	if err := p.chain.Close(ctx); err != nil {
		errs.Append(err)
	}

	// Dispatch "close"" event
	if err := p.events.DispatchOnClose(p, errs.ErrorOrNil()); err != nil {
		errs.Append(err)
	}

	p.logger.Debug(ctx, "closed encoding pipeline")
	return errs.ErrorOrNil()
}

func (p *pipeline) isClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}
