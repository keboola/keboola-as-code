package benchmark

import (
	"context"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testnode"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

// WriterBenchmark is a generic benchmark for writer.Writer.
type WriterBenchmark struct {
	// Parallelism is number of parallel write operations.
	Parallelism int
	Columns     column.Columns
	Allocate    datasize.ByteSize
	Sync        writesync.Config
	Compression compression.Config

	// DataChFactory must return the channel with table rows, the channel must be closed after the n reads.
	DataChFactory func(ctx context.Context, n int, g *RandomStringGenerator) <-chan recordctx.Context

	failedCount  *atomic.Int64
	latencySum   *atomic.Float64
	latencyCount *atomic.Int64

	apiNode        dependencies.APIScope
	apiNodeMock    dependencies.Mocked
	sourceNode     dependencies.SourceScope
	sourceNodeMock dependencies.Mocked
	writerNode     dependencies.StorageWriterScope
	writerNodeMock dependencies.Mocked

	logger log.DebugLogger
}

func (wb *WriterBenchmark) Run(b *testing.B) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create debug logger
	if testhelper.TestIsVerbose() {
		wb.logger = log.NewDebugLogger()
		wb.logger.ConnectTo(testhelper.VerboseStdout())
	} else {
		wb.logger = log.NewDebugLoggerWithoutDebugLevel()
	}

	wb.failedCount = atomic.NewInt64(0)
	wb.latencySum = atomic.NewFloat64(0)
	wb.latencyCount = atomic.NewInt64(0)
	etcdCfg := etcdhelper.TmpNamespace(b)

	// Start nodes
	wb.startNodes(b, ctx, etcdCfg)
	sinkRouter := wb.sourceNode.SinkRouter()
	storageRouter := wb.sourceNode.StorageRouter()

	// Create resource in an API node
	apiCtx := context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, wb.apiNodeMock.KeboolaProjectAPI())
	apiCtx = rollback.ContextWith(apiCtx, rollback.New(wb.apiNode.Logger()))
	wb.apiNodeMock.TestDummySinkController().FileMapping = table.Mapping{Columns: wb.Columns}
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	source.HTTP.Secret = strings.Repeat("1", 48)
	sink := dummy.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	require.NoError(b, wb.apiNode.DefinitionRepository().Branch().Create(&branch, wb.apiNode.Clock().Now(), test.ByUser()).Do(apiCtx).Err())
	require.NoError(b, wb.apiNode.DefinitionRepository().Source().Create(&source, wb.apiNode.Clock().Now(), test.ByUser(), "create").Do(apiCtx).Err())
	require.NoError(b, wb.apiNode.DefinitionRepository().Sink().Create(&sink, wb.apiNode.Clock().Now(), test.ByUser(), "create").Do(apiCtx).Err())

	// Load created slice
	slices, err := wb.apiNode.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(b, err)
	require.Len(b, slices, 1)
	slice := slices[0]
	vol, err := wb.writerNode.Volumes().Collection().Volume("my-volume-001")
	require.NoError(b, err)
	filePath := slice.LocalStorage.FileName(vol.Path(), wb.sourceNodeMock.TestConfig().NodeID)

	// Wait for initialization
	assert.EventuallyWithT(b, func(c *assert.CollectT) {
		assert.Equal(c, 1, sinkRouter.SourcesCount())
		assert.Equal(c, 1, storageRouter.SinksCount())
	}, 5*time.Second, 10*time.Millisecond)

	// Create data channel
	dataCh := wb.DataChFactory(ctx, b.N, newRandomStringGenerator())

	// Run benchmark
	b.ResetTimer()
	start := time.Now()

	// Write data in parallel, see Parallelism option.
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Start wb.Parallelism goroutines
		for i := 0; i < wb.Parallelism; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				var failedCount int64
				var latencySum float64
				var latencyCount int64

				// Read from the channel until the N rows are processed, together by all goroutines
				for record := range dataCh {
					start := time.Now()
					result := sinkRouter.DispatchToSource(sourceKey, record)
					if result.StatusCode != http.StatusOK && result.StatusCode != http.StatusAccepted {
						failedCount++
					}
					latencySum += time.Since(start).Seconds()
					latencyCount++
				}

				wb.failedCount.Add(failedCount)
				wb.latencySum.Add(latencySum)
				wb.latencyCount.Add(latencyCount)
			}()
		}
	}()
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)

	// There should be no failed write
	assert.Equal(b, int64(0), wb.failedCount.Load(), "failed writes")

	// Disable sink to force close the pipeline
	require.NoError(b, wb.apiNode.DefinitionRepository().Sink().Disable(sink.SinkKey, wb.apiNode.Clock().Now(), test.ByUser(), "reason").Do(apiCtx).Err())
	assert.EventuallyWithT(b, func(c *assert.CollectT) {
		wb.logger.AssertJSONMessages(c, `{"level":"info","message":"closed sink pipeline \"%s\": sink disabled","component":"sink.router"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Get statistics
	sliceStats, statsErr := wb.apiNode.StatisticsRepository().SliceStats(ctx, slice.SliceKey)

	// Shutdown nodes
	wb.shutdownNodes(b, ctx)

	// Get file size
	fileStat, err := os.Stat(filePath)
	require.NoError(b, err)

	// Open file
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0o640)
	require.NoError(b, err)

	// Close file
	assert.NoError(b, f.Close())

	// Check statistics
	if assert.NoError(b, statsErr) {
		assert.Equal(b, int(sliceStats.Total.RecordsCount), b.N, "records count doesn't match")
		assert.Equal(b, int64(sliceStats.Total.CompressedSize.Bytes()), fileStat.Size(), "compressed file size doesn't match")
	}

	// Report extra metrics
	b.ReportMetric(float64(b.N)/duration.Seconds(), "wr/s")
	b.ReportMetric(wb.latencySum.Load()/float64(wb.latencyCount.Load())*1000, "ms/wr")
	b.ReportMetric(sliceStats.Total.UncompressedSize.MBytes()/duration.Seconds(), "in_MB/s")
	b.ReportMetric(sliceStats.Total.CompressedSize.MBytes()/duration.Seconds(), "out_MB/s")
	b.ReportMetric(float64(sliceStats.Total.UncompressedSize)/float64(sliceStats.Total.CompressedSize), "ratio")

	// No error should be logged
	wb.logger.AssertJSONMessages(b, "")
}

func (wb *WriterBenchmark) startNodes(b *testing.B, ctx context.Context, etcdCfg etcdclient.Config) {
	b.Helper()

	wb.logger.Truncate()

	wb.startAPINode(b, ctx, etcdCfg)
	wb.startWriterNode(b, ctx, etcdCfg)
	wb.startSourceNode(b, ctx, etcdCfg)

	// Wait for connection between nodes
	assert.EventuallyWithT(b, func(c *assert.CollectT) {
		wb.logger.AssertJSONMessages(c, `{"level":"info","message":"disk writer client connected from \"%s\" to \"disk-writer\" - \"%s\"","component":"storage.router.connections.client"}`)
	}, 5*time.Second, 10*time.Millisecond)
}

func (wb *WriterBenchmark) shutdownNodes(b *testing.B, ctx context.Context) {
	b.Helper()

	// Shutdown API node
	wb.apiNode.Process().Shutdown(ctx, errors.New("bye bye API"))
	wb.apiNode.Process().WaitForShutdown()

	// Shutdown source node
	wb.sourceNode.Process().Shutdown(ctx, errors.New("bye bye source"))
	wb.sourceNode.Process().WaitForShutdown()

	// Shutdown disk writer node
	wb.writerNode.Process().Shutdown(ctx, errors.New("bye bye disk writer"))
	wb.writerNode.Process().WaitForShutdown()
}

func (wb *WriterBenchmark) startAPINode(b *testing.B, ctx context.Context, etcdCfg etcdclient.Config) {
	b.Helper()
	wb.apiNode, wb.apiNodeMock = testnode.StartAPINode(b, ctx, wb.logger, etcdCfg, wb.updateServiceConfig)
}

func (wb *WriterBenchmark) startSourceNode(b *testing.B, ctx context.Context, etcdCfg etcdclient.Config) {
	b.Helper()
	wb.sourceNode, wb.sourceNodeMock = testnode.StartSourceNode(b, ctx, wb.logger, etcdCfg, wb.updateServiceConfig)
}

func (wb *WriterBenchmark) startWriterNode(b *testing.B, ctx context.Context, etcdCfg etcdclient.Config) {
	b.Helper()
	wb.writerNode, wb.writerNodeMock = testnode.StartDiskWriterNode(b, ctx, wb.logger, etcdCfg, 1, wb.updateServiceConfig)
}

func (wb *WriterBenchmark) updateServiceConfig(cfg *config.Config) {
	cfg.Storage.Level.Local.Writer.WatchDrainFile = false
	cfg.Storage.Level.Local.Encoding.Sync = wb.Sync
	cfg.Storage.Level.Local.Encoding.Compression = wb.Compression
}
