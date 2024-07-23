package testcase

import (
	"context"
	"fmt"
	"io"
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

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
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
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/router"
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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tc.logger = log.NewDebugLogger()
	tc.logger.ConnectTo(testhelper.VerboseStdout())
	etcdCfg := etcdhelper.TmpNamespace(t)

	// Create volume directory, with volume ID file
	volumeID := volume.ID("my-volume")
	volumesPath := t.TempDir()
	volumePath := filepath.Join(volumesPath, "hdd", "001")
	require.NoError(t, os.MkdirAll(volumePath, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(volumePath, volume.IDFile), []byte(volumeID), 0o600))

	// Start nodes
	tc.startNodes(t, ctx, etcdCfg, volumesPath)
	sinkRouter := tc.sourceNode.SinkRouter()

	// Create resource in an API node
	apiScp, apiMock := tc.startAPINode(t, etcdCfg)
	apiCtx := context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, apiMock.KeboolaProjectAPI())
	apiCtx = rollback.ContextWith(apiCtx, rollback.New(apiScp.Logger()))
	transport := apiMock.MockedHTTPTransport()
	bridgeTest.MockTokenStorageAPICalls(t, transport)
	bridgeTest.MockBucketStorageAPICalls(t, transport)
	bridgeTest.MockTableStorageAPICalls(t, transport)
	bridgeTest.MockFileStorageAPICalls(t, apiMock.Clock(), transport)
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	source.HTTP.Secret = strings.Repeat("1", 48)
	sink := test.NewKeboolaTableSink(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	sink.Table.Mapping = table.Mapping{Columns: tc.Columns}
	require.NoError(t, apiScp.DefinitionRepository().Branch().Create(&branch, apiScp.Clock().Now(), test.ByUser()).Do(apiCtx).Err())
	require.NoError(t, apiScp.DefinitionRepository().Source().Create(&source, apiScp.Clock().Now(), test.ByUser(), "create").Do(apiCtx).Err())
	require.NoError(t, apiScp.DefinitionRepository().Sink().Create(&sink, apiScp.Clock().Now(), test.ByUser(), "create").Do(apiCtx).Err())

	// Load created slice
	slices, err := apiScp.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 1)
	slice := slices[0]
	filePath := slice.LocalStorage.FileName(volumePath, tc.sourceNodeMock.TestConfig().NodeID)

	// Wait for pipeline initialization
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		// Messages order can be random
		tc.logger.AssertJSONMessages(c, `{"level":"debug","message":"synced to revision %s","component":"sink.router"}`)
		tc.logger.AssertJSONMessages(c, `{"level":"debug","message":"synced to revision %s","component":"storage.router"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Write all rows batches
	rowsCount := 0
	for i, batch := range tc.Data {
		rowsCount += len(batch.Records)

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

	// Check written data
	tc.Validator(t, string(content))

	// Check statistics
	sliceStats, err := apiScp.StatisticsRepository().SliceStats(ctx, slice.SliceKey)
	if assert.NoError(t, err) {
		assert.Equal(t, int(sliceStats.Total.RecordsCount), rowsCount, "records count doesn't match")
		assert.Equal(t, int64(sliceStats.Total.CompressedSize.Bytes()), fileStat.Size(), "compressed file size doesn't match")
		assert.Equal(t, int(sliceStats.Total.UncompressedSize.Bytes()), len(content), "uncompressed file size doesn't match")
	}

	// Shutdown API node
	apiScp.Process().Shutdown(ctx, errors.New("bye bye API"))
	apiScp.Process().WaitForShutdown()

	// No error should be logged
	tc.logger.AssertJSONMessages(t, "")
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

func (tc *WriterTestCase) startNodes(t *testing.T, ctx context.Context, etcdCfg etcdclient.Config, volumesPath string) {
	t.Helper()

	tc.logger.Truncate()

	// Start disk in node
	tc.startWriterNode(t, ctx, etcdCfg, volumesPath)

	// Start source node
	tc.startSourceNode(t, etcdCfg)

	// Wait for connection between nodes
	assert.EventuallyWithT(t, func(c *assert.CollectT) {
		tc.logger.AssertJSONMessages(c, `{"level":"info","message":"disk writer client connected from \"%s\" to \"disk-writer\" - \"%s\"","component":"storage.router.connections.client"}`)
	}, 5*time.Second, 10*time.Millisecond)
}

func (tc *WriterTestCase) shutdownNodes(t *testing.T, ctx context.Context) {
	t.Helper()

	// Close source node
	tc.sourceNode.Process().Shutdown(ctx, errors.New("bye bye source"))
	tc.sourceNode.Process().WaitForShutdown()

	// Close disk writer node
	tc.writerNode.Process().Shutdown(ctx, errors.New("bye bye disk writer"))
	tc.writerNode.Process().WaitForShutdown()
}

func (tc *WriterTestCase) startAPINode(t *testing.T, etcdCfg etcdclient.Config) (dependencies.APIScope, dependencies.Mocked) {
	t.Helper()

	return dependencies.NewMockedAPIScopeWithConfig(
		t,
		func(cfg *config.Config) {
			tc.updateServiceConfig(cfg)
			cfg.NodeID = "api"
			cfg.API.Listen = fmt.Sprintf("0.0.0.0:%d", netutils.FreePortForTest(t))
		},
		commonDeps.WithDebugLogger(tc.logger),
		commonDeps.WithEtcdConfig(etcdCfg),
	)
}

func (tc *WriterTestCase) startSourceNode(t *testing.T, etcdCfg etcdclient.Config) {
	t.Helper()

	tc.sourceNode, tc.sourceNodeMock = dependencies.NewMockedSourceScopeWithConfig(
		t,
		func(cfg *config.Config) {
			tc.updateServiceConfig(cfg)
			cfg.NodeID = "source"
		},
		commonDeps.WithDebugLogger(tc.logger),
		commonDeps.WithEtcdConfig(etcdCfg),
	)
}

func (tc *WriterTestCase) startWriterNode(t *testing.T, ctx context.Context, etcdCfg etcdclient.Config, volumesPath string) {
	t.Helper()

	tc.writerNode, tc.writerNodeMock = dependencies.NewMockedStorageWriterScopeWithConfig(
		t,
		func(cfg *config.Config) {
			tc.updateServiceConfig(cfg)
			cfg.NodeID = "disk-writer"
			cfg.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", netutils.FreePortForTest(t))
			cfg.Storage.VolumesPath = volumesPath
		},
		commonDeps.WithDebugLogger(tc.logger),
		commonDeps.WithEtcdConfig(etcdCfg),
	)

	require.NoError(t, writernode.Start(ctx, tc.writerNode, tc.writerNodeMock.TestConfig()))
}

func (tc *WriterTestCase) updateServiceConfig(cfg *config.Config) {
	cfg.Hostname = "localhost"
	cfg.Storage.Level.Local.Writer.WatchDrainFile = false
	cfg.Storage.Level.Local.Encoding.Sync = tc.Sync
	cfg.Storage.Level.Local.Encoding.Compression = tc.Compression
}
