package benchmark

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	bridgeTest "github.com/keboola/keboola-as-code/internal/pkg/service/stream/keboolasink/bridge/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/writernode"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
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

	wb.logger = log.NewDebugLogger()
	wb.logger.ConnectTo(testhelper.VerboseStdout())
	wb.failedCount = atomic.NewInt64(0)
	wb.latencySum = atomic.NewFloat64(0)
	wb.latencyCount = atomic.NewInt64(0)
	etcdCfg := etcdhelper.TmpNamespace(b)

	// Create volume directory, with volume ID file
	volumeID := volume.ID("my-volume")
	volumesPath := b.TempDir()
	volumePath := filepath.Join(volumesPath, "hdd", "001")
	require.NoError(b, os.MkdirAll(volumePath, 0o700))
	require.NoError(b, os.WriteFile(filepath.Join(volumePath, volume.IDFile), []byte(volumeID), 0o600))

	// Start nodes
	wb.startNodes(b, ctx, etcdCfg, volumesPath)
	sinkRouter := wb.sourceNode.SinkRouter()

	// Create resource in an API node
	apiScp, apiMock := wb.startAPINode(b, etcdCfg)
	apiCtx := context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, apiMock.KeboolaProjectAPI())
	apiCtx = rollback.ContextWith(apiCtx, rollback.New(apiScp.Logger()))
	transport := apiMock.MockedHTTPTransport()
	bridgeTest.MockTokenStorageAPICalls(b, transport)
	bridgeTest.MockBucketStorageAPICalls(b, transport)
	bridgeTest.MockTableStorageAPICalls(b, transport)
	bridgeTest.MockFileStorageAPICalls(b, apiMock.Clock(), transport)
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	source.HTTP.Secret = strings.Repeat("1", 48)
	sink := test.NewKeboolaTableSink(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	sink.Table.Mapping = table.Mapping{Columns: wb.Columns}
	require.NoError(b, apiScp.DefinitionRepository().Branch().Create(&branch, apiScp.Clock().Now(), test.ByUser()).Do(apiCtx).Err())
	require.NoError(b, apiScp.DefinitionRepository().Source().Create(&source, apiScp.Clock().Now(), test.ByUser(), "create").Do(apiCtx).Err())
	require.NoError(b, apiScp.DefinitionRepository().Sink().Create(&sink, apiScp.Clock().Now(), test.ByUser(), "create").Do(apiCtx).Err())

	// Load created slice
	slices, err := apiScp.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(b, err)
	require.Len(b, slices, 1)
	slice := slices[0]
	filePath := slice.LocalStorage.FileName(volumePath, wb.sourceNodeMock.TestConfig().NodeID)

	// Wait for pipeline initialization
	assert.EventuallyWithT(b, func(c *assert.CollectT) {
		// Messages order can be random
		wb.logger.AssertJSONMessages(c, `{"level":"debug","message":"synced to revision %s","component":"sink.router"}`)
		// wb.logger.AssertJSONMessages(c, `{"level":"debug","message":"synced to revision %s","component":"storage.router"}`)
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
	sliceStats, err := apiScp.StatisticsRepository().SliceStats(ctx, slice.SliceKey)
	if assert.NoError(b, err) {
		assert.Equal(b, int(sliceStats.Total.RecordsCount), b.N, "records count doesn't match")
		assert.Equal(b, int64(sliceStats.Total.CompressedSize.Bytes()), fileStat.Size(), "compressed file size doesn't match")
	}

	// Report extra metrics
	b.ReportMetric(float64(b.N)/duration.Seconds(), "wr/s")
	b.ReportMetric(wb.latencySum.Load()/float64(wb.latencyCount.Load())*1000, "ms/wr")
	b.ReportMetric(sliceStats.Total.UncompressedSize.MBytes()/duration.Seconds(), "in_MB/s")
	b.ReportMetric(sliceStats.Total.CompressedSize.MBytes()/duration.Seconds(), "out_MB/s")
	b.ReportMetric(float64(sliceStats.Total.UncompressedSize)/float64(sliceStats.Total.CompressedSize), "ratio")

	// Shutdown API node
	apiScp.Process().Shutdown(ctx, errors.New("bye bye API"))
	apiScp.Process().WaitForShutdown()

	// No error should be logged
	wb.logger.AssertJSONMessages(b, "")
}

func (wb *WriterBenchmark) startNodes(b *testing.B, ctx context.Context, etcdCfg etcdclient.Config, volumesPath string) {
	b.Helper()

	wb.logger.Truncate()

	// Start disk in node
	wb.startWriterNode(b, ctx, etcdCfg, volumesPath)

	// Start source node
	wb.startSourceNode(b, etcdCfg)

	// Wait for connection between nodes
	assert.EventuallyWithT(b, func(c *assert.CollectT) {
		wb.logger.AssertJSONMessages(c, `{"level":"info","message":"disk writer client connected from \"%s\" to \"disk-writer\" - \"%s\"","component":"storage.router.connections.client"}`)
	}, 5*time.Second, 10*time.Millisecond)
}

func (wb *WriterBenchmark) shutdownNodes(b *testing.B, ctx context.Context) {
	b.Helper()

	// Close source node
	wb.sourceNode.Process().Shutdown(ctx, errors.New("bye bye source"))
	wb.sourceNode.Process().WaitForShutdown()

	// Close disk writer node
	wb.writerNode.Process().Shutdown(ctx, errors.New("bye bye disk writer"))
	wb.writerNode.Process().WaitForShutdown()
}

func (wb *WriterBenchmark) startAPINode(b *testing.B, etcdCfg etcdclient.Config) (dependencies.APIScope, dependencies.Mocked) {
	b.Helper()

	return dependencies.NewMockedAPIScopeWithConfig(
		b,
		func(cfg *config.Config) {
			wb.updateServiceConfig(cfg)
			cfg.NodeID = "api"
			cfg.API.Listen = fmt.Sprintf("0.0.0.0:%d", netutils.FreePortForTest(b))
		},
		commonDeps.WithDebugLogger(wb.logger),
		commonDeps.WithEtcdConfig(etcdCfg),
	)
}

func (wb *WriterBenchmark) startSourceNode(b *testing.B, etcdCfg etcdclient.Config) {
	b.Helper()

	wb.sourceNode, wb.sourceNodeMock = dependencies.NewMockedSourceScopeWithConfig(
		b,
		func(cfg *config.Config) {
			wb.updateServiceConfig(cfg)
			cfg.NodeID = "source"
		},
		commonDeps.WithDebugLogger(wb.logger),
		commonDeps.WithEtcdConfig(etcdCfg),
	)
}

func (wb *WriterBenchmark) startWriterNode(b *testing.B, ctx context.Context, etcdCfg etcdclient.Config, volumesPath string) {
	b.Helper()

	wb.writerNode, wb.writerNodeMock = dependencies.NewMockedStorageWriterScopeWithConfig(
		b,
		func(cfg *config.Config) {
			wb.updateServiceConfig(cfg)
			cfg.NodeID = "disk-writer"
			cfg.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", netutils.FreePortForTest(b))
			cfg.Storage.VolumesPath = volumesPath
		},
		// commonDeps.WithDebugLogger(wb.logger),
		commonDeps.WithEtcdConfig(etcdCfg),
	)

	require.NoError(b, writernode.Start(ctx, wb.writerNode, wb.writerNodeMock.TestConfig()))
}

func (wb *WriterBenchmark) updateServiceConfig(cfg *config.Config) {
	cfg.Hostname = "localhost"
	cfg.Storage.Level.Local.Writer.WatchDrainFile = false
	cfg.Storage.Level.Local.Encoding.Sync = wb.Sync
	cfg.Storage.Level.Local.Encoding.Compression = wb.Compression
}
