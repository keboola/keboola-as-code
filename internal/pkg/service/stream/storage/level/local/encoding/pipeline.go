package encoding

import (
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ccoveille/go-safecast/v2"
	"github.com/jonboulle/clockwork"
	"go.opentelemetry.io/otel/attribute"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/ctxattr"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/chunk"
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
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/telemetry"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

// Pipeline processes record values using the configured Encoder and compression.
type Pipeline interface {
	StatisticsProvider

	SliceKey() model.SliceKey

	// IsReady is used for load balancing, to detect health pipelines.
	IsReady() bool
	// NetworkOutput returns the network output of the pipeline.
	NetworkOutput() rpc.NetworkOutput
	// WriteRecord blocks until the record is written and synced to the local storage, if the wait is enabled.
	WriteRecord(record recordctx.Context) (int, error)
	// Events provides listening to the writer lifecycle.
	Events() *events.Events[Pipeline]
	// Close the writer and sync data to the disk.
	Close(ctx context.Context) error
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
	logger    log.Logger
	sliceKey  model.SliceKey
	events    *events.Events[Pipeline]
	flushLock sync.RWMutex

	encoder      encoder.Encoder
	chain        *writechain.Chain
	syncer       *writesync.Syncer
	chunks       *chunk.Writer
	connections  *connection.Manager
	telemetry    telemetry.Telemetry
	localStorage localModel.Slice
	network      rpc.NetworkOutput
	withBackup   bool
	closeFunc    func(ctx context.Context, cause string)

	readyLock sync.RWMutex
	ready     bool

	// closed blocks new writes
	closed chan struct{}
	// writeWg waits for in-progress writes without waiting for sync.
	writeWg sync.WaitGroup
	// writeCompletedWg waits until all Write method calls are completed, it includes waiting for the sync.
	writeCompletedWg sync.WaitGroup
	// writeWg waits for in-progress writes before Close
	chunksWg sync.WaitGroup

	acceptedWrites    *count.Counter
	completedWrites   *count.Counter
	compressedMeter   *size.Meter
	uncompressedMeter *size.Meter
}

func newPipeline(
	ctx context.Context,
	logger log.Logger,
	clk clockwork.Clock,
	sliceKey model.SliceKey,
	telemetry telemetry.Telemetry,
	connections *connection.Manager,
	mappingCfg table.Mapping,
	encodingCfg encoding.Config,
	localStorage localModel.Slice,
	events *events.Events[Pipeline],
	withBackup bool,
	closeFunc func(ctx context.Context, cause string),
	network rpc.NetworkOutput,
) (out Pipeline, err error) {
	p := &pipeline{
		logger:       logger.WithComponent("encoding.pipeline"),
		telemetry:    telemetry,
		connections:  connections,
		sliceKey:     sliceKey,
		events:       events.Clone(), // clone passed events, so additional pipeline specific listeners can be added
		ready:        true,
		closeFunc:    closeFunc,
		localStorage: localStorage,
		withBackup:   withBackup,
		closed:       make(chan struct{}),
	}

	// Open remote RPC file
	// The disk writer node can notify us of its termination. In that case, we have to gracefully close the pipeline, see Close method.
	p.network = network
	if network == nil {
		p.network, err = rpc.OpenNetworkFile(
			ctx,
			p.logger,
			p.telemetry,
			p.connections,
			p.sliceKey,
			p.localStorage,
			p.withBackup,
			p.closeFunc,
		)
		if err != nil {
			return nil, errors.PrefixErrorf(err, "cannot open network file for new slice pipeline")
		}
	}

	ctx = context.WithoutCancel(ctx)
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
			if p.network != nil {
				_ = p.network.Close(ctx)
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

	// Setup chunks sending to the network file
	{
		bytes, err := safecast.Convert[int](encodingCfg.MaxChunkSize.Bytes())
		if err != nil {
			return nil, err
		}
		p.chunks = chunk.NewWriter(p.logger, bytes)

		// Process each completed chunk.
		// Block the close until all chunks are written to the network output.
		p.chunksWg.Go(func() {
			p.processChunks(ctx, clk, encodingCfg)
		})
	}

	// Create empty chain of writers to the file
	// The chain is built from the end, from the chunks buffer, to the CSV writer at the start
	{
		p.chain = writechain.New(logger, p.chunks)
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
				_, err = p.chain.PrependWriterOrErr(func(w io.Writer) (io.Writer, error) {
					bytes, err := safecast.Convert[int](encodingCfg.InputBuffer.Bytes())
					if err != nil {
						return nil, err
					}
					return limitbuffer.New(w, bytes), nil
				})
				if err != nil {
					return nil, err
				}
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
		p.syncer = syncerFactory.NewSyncer(ctx, logger, clk, encodingCfg.Sync, p, p)
	}

	// Create encoder.
	// It is entrypoint of the writers chain.
	{
		// Get factory
		var encoderFactory encoder.Factory = encoder.DefaultFactory{}
		if encodingCfg.Encoder.OverrideEncoderFactory != nil {
			encoderFactory = encodingCfg.Encoder.OverrideEncoderFactory
		}

		// Create encoder
		p.encoder, err = encoderFactory.NewEncoder(encodingCfg.Encoder, mappingCfg, p.chain, p.syncer.Notifier)
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
	// Network is not working
	if !p.network.IsReady() {
		return false
	}

	// ready == false means: too many failed Flush ops
	p.readyLock.RLock()
	defer p.readyLock.RUnlock()
	return p.ready
}

func (p *pipeline) NetworkOutput() rpc.NetworkOutput {
	return p.network
}

func (p *pipeline) WriteRecord(record recordctx.Context) (int, error) {
	timestamp := record.Timestamp()

	// Block Close method
	p.writeWg.Add(1)
	p.writeCompletedWg.Add(1)
	defer p.writeCompletedWg.Done()

	// Check if the writer is closed
	if p.isClosed() {
		return 0, errors.New(`writer is closed`)
	}

	// Format and write table row
	p.flushLock.RLock()
	writeRecordResult, err := p.encoder.WriteRecord(record)
	p.writeWg.Done()
	p.flushLock.RUnlock()
	if err != nil {
		return writeRecordResult.N, err
	}

	// Increments number of high-level writes in progressd
	p.acceptedWrites.Add(timestamp, 1)

	// Wait for sync and return sync error, if any
	if err := writeRecordResult.Notifier.Wait(record.Ctx()); err != nil {
		return writeRecordResult.N, errors.PrefixError(err, "error when waiting for sync")
	}

	// Increase the count of successful writes
	p.completedWrites.Add(timestamp, 1)
	return writeRecordResult.N, nil
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

// Flush all internal buffers to the NetworkOutput.
// The method is called periodically by the writesync.Syncer.
func (p *pipeline) Flush(ctx context.Context) error {
	p.flushLock.Lock()
	defer p.flushLock.Unlock()

	ctx, cancel := context.WithTimeoutCause(ctx, 30*time.Second, errors.New("pipeline flush timeout"))
	defer cancel()

	// Flush internal buffers, the active chunk is completed.
	if err := p.chain.Flush(ctx); err != nil {
		return err
	}

	// We have chunks, what next?
	select {
	case <-p.chunks.WaitAllProcessedCh():
		return nil
	case <-ctx.Done():
		return errors.PrefixErrorf(ctx.Err(), "cannot flush pipeline %d chunks", p.chunks.CompletedChunks())
	}
}

// Sync at first Flush all internal buffers to the NetworkOutput.
// And then calls NetworkOutput.Sync to sync OS cache to the physical disk, in the disk writer node.
// The method is called periodically by the writesync.Syncer.
func (p *pipeline) Sync(ctx context.Context) error {
	if err := p.Flush(ctx); err != nil {
		return err
	}
	return p.network.Sync(ctx)
}

func (p *pipeline) Close(ctx context.Context) error {
	p.logger.Debug(ctx, "closing encoding pipeline")

	// Close only once
	if p.isClosed() {
		return errors.New("encoding pipeline is already closed")
	}
	close(p.closed)

	errs := errors.NewMultiError()

	// Wait for in-progress writes.
	p.writeWg.Wait()

	// Stop syncer, it triggers also the last sync to unblock write sync notifiers.
	if err := p.syncer.Stop(ctx); err != nil {
		errs.Append(err)
	}

	// Wait for all Write method calls, it includes waiting for the sync.
	p.writeCompletedWg.Wait()

	// Close writers chain, it closes all writers and generates the last chunk.
	if err := p.chain.Close(ctx); err != nil {
		errs.Append(err)
	}

	// Wait until all chunks are written
	p.chunksWg.Wait()

	// Close remote network file
	if err := p.network.Close(ctx); err != nil {
		errs.Append(err)
	}

	// Dispatch "close"" event
	if err := p.events.DispatchOnClose(p, errs.ErrorOrNil()); err != nil {
		errs.Append(err)
	}

	p.logger.Debug(ctx, "closed encoding pipeline")
	return errs.ErrorOrNil()
}

func (p *pipeline) processChunks(ctx context.Context, clk clockwork.Clock, encodingCfg encoding.Config) {
	b := newChunkBackoff()
	for {
		// The channel is unblocked if there is an unprocessed chunk,
		// or all chunks have been processed and the writer is closed.
		<-p.chunks.WaitForChunkCh()

		// All done
		if p.chunks.CompletedChunks() == 0 {
			return
		}

		// Write all chunks to the network output
		err := p.chunks.ProcessCompletedChunks(func(chunk *chunk.Chunk) error {
			if !p.network.IsReady() {
				return errors.New("network is not ready")
			}

			length, err := safecast.Convert[uint64](chunk.Len())
			if err != nil {
				return err
			}
			l := datasize.ByteSize(length)
			if _, err := p.network.Write(ctx, chunk.Aligned(), chunk.Bytes()); err != nil {
				if strings.HasSuffix(err.Error(), os.ErrClosed.Error()) {
					// Open remote RPC file
					p.network, err = rpc.OpenNetworkFile(
						ctx,
						p.logger,
						p.telemetry,
						p.connections,
						p.sliceKey,
						p.localStorage,
						p.withBackup,
						p.closeFunc,
					)
					if err != nil {
						return errors.PrefixErrorf(err, "cannot open network file for new slice pipeline")
					}
				}

				p.logger.Debugf(ctx, "chunk write failed, size %q: %s", l.String(), err)
				return err
			}
			p.logger.Debugf(ctx, "chunk written, size %q", l.String())
			return nil
		})
		if err != nil {
			// Mark the pipeline not ready
			cnt := p.chunks.CompletedChunks()
			if cnt >= encodingCfg.FailedChunksThreshold {
				p.readyLock.Lock()
				p.ready = false
				p.readyLock.Unlock()
			}

			// Wait before retry
			delay := b.NextBackOff()
			p.logger.Warnf(ctx, "chunks write failed: %s, waiting %s, chunks count = %d", err, delay, cnt)
			<-clk.After(delay)
			continue
		}

		// All chunks have been written, mark the pipeline ready
		p.readyLock.Lock()
		p.ready = true
		p.readyLock.Unlock()
	}
}

func (p *pipeline) isClosed() bool {
	select {
	case <-p.closed:
		return true
	default:
		return false
	}
}
