package testcase

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ccoveille/go-safecast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/table/column"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/router"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/writesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testnode"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type WriterTestCase struct {
	Name              string
	Columns           column.Columns
	Allocate          datasize.ByteSize
	Sync              writesync.Config
	Compression       compression.Config
	DisableValidation bool

	Data        []RecordsBatch
	FileDecoder func(t *testing.T, r io.Reader) io.Reader
	Validator   func(t *testing.T, fileContent string)

	apiNode        dependencies.APIScope
	apiNodeMock    dependencies.Mocked
	sourceNode     dependencies.SourceScope
	sourceNodeMock dependencies.Mocked
	writerNode     dependencies.StorageWriterScope
	writerNodeMock dependencies.Mocked

	logger log.DebugLogger
}

type RecordsBatch struct {
	Parallel bool
	Records  []recordctx.Context
}

// nolint:thelper // false positive
func (tc *WriterTestCase) Run(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeoutCause(t.Context(), 30*time.Second, errors.New("test timeout"))
	defer cancel()

	tc.logger = log.NewDebugLogger()
	tc.logger.ConnectTo(testhelper.VerboseStdout())
	etcdCfg := etcdhelper.TmpNamespace(t)

	// Start nodes
	tc.startNodes(t, ctx, etcdCfg)
	sinkRouter := tc.sourceNode.SinkRouter()

	// Create resource in an API node
	apiCtx := context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, tc.apiNodeMock.KeboolaProjectAPI())
	apiCtx = rollback.ContextWith(apiCtx, rollback.New(tc.apiNode.Logger()))
	tc.apiNodeMock.TestDummySinkController().FileMapping = table.Mapping{Columns: tc.Columns}
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	sink := dummy.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	require.NoError(t, tc.apiNode.DefinitionRepository().Branch().Create(&branch, tc.apiNode.Clock().Now(), test.ByUser()).Do(apiCtx).Err())
	require.NoError(t, tc.apiNode.DefinitionRepository().Source().Create(&source, tc.apiNode.Clock().Now(), test.ByUser(), "create").Do(apiCtx).Err())
	require.NoError(t, tc.apiNode.DefinitionRepository().Sink().Create(&sink, tc.apiNode.Clock().Now(), test.ByUser(), "create").Do(apiCtx).Err())

	// Load created slice
	slices, err := tc.apiNode.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 1)
	slice := slices[0]
	vol, err := tc.writerNode.Volumes().Collection().Volume("my-volume-001")
	require.NoError(t, err)
	filePath := slice.LocalStorage.FileName(vol.Path(), tc.sourceNodeMock.TestConfig().NodeID)

	// Wait for pipeline initialization
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		// Messages order can be random
		tc.logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %s","component":"sink.router"}`)
		tc.logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %s","component":"storage.router"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Write all rows batches
	var rowsCount uint64 = 0
	for i, batch := range tc.Data {
		rowsCount += uint64(len(batch.Records))

		done := make(chan struct{})

		// There are two write modes
		if batch.Parallel {
			// Write rows from the set in parallel
			wg := &sync.WaitGroup{}
			for _, record := range batch.Records {
				wg.Add(1)
				go func() {
					defer wg.Done()
					tc.assertResult(t, sinkRouter.DispatchToSource(sourceKey, record))
				}()
			}
			go func() {
				wg.Wait()
				close(done)
			}()
		} else {
			// Write rows from the set sequentially
			go func() {
				defer close(done)
				for _, record := range batch.Records {
					tc.assertResult(t, sinkRouter.DispatchToSource(sourceKey, record))
				}
			}()
		}

		// Wait for all rows from the batch to be written
		select {
		case <-time.After(2 * time.Second):
			require.Fail(t, fmt.Sprintf(`timeout when waiting for batch %d02`, i+1))
		case <-done:
			t.Logf(`set %02d written`, i+1)
		}

		//// Simulate pod failure, restart in
		// if i == len(tc.Data)-2 {
		//	tc.shutdownNodes(t, ctx, diskWriterNode, sourceNode)
		//	diskWriterNode, sourceNode = tc.startNodes(t, ctx, etcdCfg, volumesPath)
		//	router = sourceNode.SinkRouter()
		//}
	}

	// Disable sink to force close the pipeline
	require.NoError(t, tc.apiNode.DefinitionRepository().Sink().Disable(sink.SinkKey, tc.apiNode.Clock().Now(), test.ByUser(), "reason").Do(apiCtx).Err())
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		tc.logger.AssertJSONMessages(c, `{"level":"info","message":"closed sink pipeline:%s","component":"sink.router"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Get statistics
	sliceStats, statsErr := tc.apiNode.StatisticsRepository().SliceStats(ctx, slice.SliceKey)

	// Shutdown nodes
	tc.shutdownNodes(t, ctx)

	// Get file size
	fileStat, err := os.Stat(filePath)
	require.NoError(t, err)

	// Open file
	f, err := os.OpenFile(filePath, os.O_RDONLY, 0o640)
	require.NoError(t, err)

	// Create file reader
	var reader io.Reader = f
	if tc.FileDecoder != nil {
		reader = tc.FileDecoder(t, reader)
	}

	// Read file content
	content, err := io.ReadAll(reader)
	assert.NoError(t, err)

	// Close file
	assert.NoError(t, f.Close())

	// Check stats
	if assert.NoError(t, statsErr) {
		assert.Equal(t, sliceStats.Total.RecordsCount, rowsCount, "records count doesn't match")
		size, err := safecast.ToUint64(fileStat.Size())
		assert.NoError(t, err)
		assert.Equal(t, sliceStats.Total.CompressedSize.Bytes(), size, "compressed file size doesn't match")
		assert.Equal(t, sliceStats.Total.UncompressedSize.Bytes(), uint64(len(content)), "uncompressed file size doesn't match")
	}

	// Check written data
	tc.Validator(t, string(content))

	// No error should be logged
	tc.logger.AssertNoErrorMessage(t)
}

func (tc *WriterTestCase) assertResult(t *testing.T, result *router.SourceResult) {
	t.Helper()
	if assert.Equal(t, "", result.ErrorName, result.Message) {
		if tc.Sync.Wait {
			if !assert.Equal(t, http.StatusOK, result.StatusCode) {
				t.Log(json.MustEncodeString(result, true))
			}
		} else {
			if !assert.Equal(t, http.StatusAccepted, result.StatusCode) {
				t.Log(json.MustEncodeString(result, true))
			}
		}
	} else {
		t.Log(json.MustEncodeString(result, true))
	}
}

func (tc *WriterTestCase) startNodes(t *testing.T, ctx context.Context, etcdCfg etcdclient.Config) {
	t.Helper()

	tc.logger.Truncate()

	tc.startAPINode(t, ctx, etcdCfg)
	tc.startWriterNode(t, ctx, etcdCfg)
	tc.startSourceNode(t, ctx, etcdCfg)

	// Wait for connection between nodes
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		tc.logger.AssertJSONMessages(c, `{"level":"info","message":"disk writer client connected from \"%s\" to \"disk-writer\" - \"%s\"","component":"storage.router.connections.client.transport"}`)
	}, 5*time.Second, 10*time.Millisecond)
}

func (tc *WriterTestCase) shutdownNodes(t *testing.T, ctx context.Context) {
	t.Helper()

	// Shutdown API node
	tc.apiNode.Process().Shutdown(ctx, errors.New("bye bye API"))
	tc.apiNode.Process().WaitForShutdown()

	// Shutdown source node
	tc.sourceNode.Process().Shutdown(ctx, errors.New("bye bye source"))
	tc.sourceNode.Process().WaitForShutdown()

	// Shutdown disk writer node
	tc.writerNode.Process().Shutdown(ctx, errors.New("bye bye disk writer"))
	tc.writerNode.Process().WaitForShutdown()
}

func (tc *WriterTestCase) startAPINode(t *testing.T, ctx context.Context, etcdCfg etcdclient.Config) {
	t.Helper()
	tc.apiNode, tc.apiNodeMock = testnode.StartAPINode(t, ctx, tc.logger, etcdCfg, tc.updateServiceConfig)
}

func (tc *WriterTestCase) startSourceNode(t *testing.T, ctx context.Context, etcdCfg etcdclient.Config) {
	t.Helper()
	tc.sourceNode, tc.sourceNodeMock = testnode.StartSourceNode(t, ctx, tc.logger, etcdCfg, tc.updateServiceConfig)
}

func (tc *WriterTestCase) startWriterNode(t *testing.T, ctx context.Context, etcdCfg etcdclient.Config) {
	t.Helper()
	tc.writerNode, tc.writerNodeMock = testnode.StartDiskWriterNode(t, ctx, tc.logger, etcdCfg, 1, tc.updateServiceConfig)
}

func (tc *WriterTestCase) updateServiceConfig(cfg *config.Config) {
	cfg.Storage.Level.Local.Writer.WatchDrainFile = false
	cfg.Storage.Level.Local.Encoding.Sync = tc.Sync
	cfg.Storage.Level.Local.Encoding.Compression = tc.Compression
}
