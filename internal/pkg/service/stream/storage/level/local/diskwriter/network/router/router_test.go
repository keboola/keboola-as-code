package router_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/sink/pipeline"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/closesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testnode"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

func TestRouter_UpdatePipelinesOnSlicesChange(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := log.NewDebugLogger()
	logger.ConnectTo(testhelper.VerboseStdout())
	etcdCfg := etcdhelper.TmpNamespace(t)

	// Start disk writer node
	volumesCount := 2
	writerNode, _ := testnode.StartDiskWriterNode(t, logger, etcdCfg, volumesCount, nil)

	// Create coordinator, to check reported revisions
	svcScope, _ := dependencies.NewMockedServiceScope(t, commonDeps.WithEtcdConfig(etcdCfg))
	coordinator, err := closesync.NewCoordinatorNode(svcScope)
	require.NoError(t, err)

	// Helper
	waitForMinRevInUse := func(t *testing.T, r int64) {
		t.Helper()
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.GreaterOrEqual(c, coordinator.MinRevInUse(), r)
		}, 5*time.Second, 10*time.Millisecond)
	}

	// Wait for volumes registration
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"info","message":"disk writer listening on \"%s\""}`)
		logger.AssertJSONMessages(c, `{"level":"info","message":"registered \"2\" volumes"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Start a source node
	sourceScp, mock := testnode.StartSourceNode(t, logger, etcdCfg, nil)
	clk := mock.Clock()

	// Wait for connection between nodes
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"info","message":"disk writer client connected from \"%s\" to \"disk-writer\" - \"%s\""}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Fixtures
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	source.HTTP.Secret = strings.Repeat("1", 48)
	sink := test.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	sink.Config = testconfig.LocalVolumeConfig(2, []string{"hdd"})
	require.NoError(t, sourceScp.DefinitionRepository().Branch().Create(&branch, clk.Now(), test.ByUser()).Do(ctx).Err())
	require.NoError(t, sourceScp.DefinitionRepository().Source().Create(&source, clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	sinkResult := sourceScp.DefinitionRepository().Sink().Create(&sink, clk.Now(), test.ByUser(), "create").Do(ctx)
	require.NoError(t, sinkResult.Err())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"sink.router"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Open pipeline, send some data
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()
	body := io.NopCloser(strings.NewReader(`{"foo":"bar"}`))
	req := (&http.Request{Body: body}).WithContext(reqCtx)
	result := sourceScp.SinkRouter().DispatchToSource(sourceKey, recordctx.FromHTTP(clk.Now(), req))
	require.Empty(t, result.ErrorName, result.Message)
	require.Equal(t, http.StatusOK, result.StatusCode, result.Message)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"debug","message":"opened balanced pipeline to 2 slices, sink \"123/111/my-source/my-sink\"","component":"storage.router"}
`)
	}, 5*time.Second, 10*time.Millisecond)
	waitForMinRevInUse(t, sinkResult.Header().Revision)

	// Rotate file/slices
	rotateResult := sourceScp.StorageRepository().File().Rotate(sink.SinkKey, clk.Now()).Do(ctx)
	require.NoError(t, rotateResult.Err())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"debug","message":"updated balanced pipeline, 2 opened slices, 2 closed slices, sink \"123/111/my-source/my-sink\"","component":"storage.router"}
`)
	}, 5*time.Second, 10*time.Millisecond)
	waitForMinRevInUse(t, rotateResult.Header().Revision)

	// Disable sink - close files/slices
	disableResult := sourceScp.DefinitionRepository().Sink().Disable(sink.SinkKey, clk.Now(), test.ByUser(), "reason").Do(ctx)
	require.NoError(t, disableResult.Err())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"debug","message":"closed balanced pipeline to 2 slices, sink \"123/111/my-source/my-sink\"","component":"storage.router"}
`)
	}, 5*time.Second, 10*time.Millisecond)
	waitForMinRevInUse(t, disableResult.Header().Revision)

	// Shutdown the source node
	sourceScp.Process().Shutdown(ctx, errors.New("bye bye"))
	sourceScp.Process().WaitForShutdown()
	waitForMinRevInUse(t, closesync.NoSourceNode)

	// Shutdown the writer node
	writerNode.Process().Shutdown(ctx, errors.New("bye bye"))
	writerNode.Process().WaitForShutdown()
}

type TestPipeline struct {
	logger     io.Writer
	sliceKey   model.SliceKey
	Name       string
	Ready      bool
	WriteError error
	CloseError error
}

func NewTestPipeline(name string, sliceKey model.SliceKey, logger io.Writer) *TestPipeline {
	return &TestPipeline{
		Name:     name,
		sliceKey: sliceKey,
		logger:   logger,
		Ready:    true,
	}
}

func (p *TestPipeline) SliceKey() model.SliceKey {
	return p.sliceKey
}

func (p *TestPipeline) IsReady() bool {
	return p.Ready
}

func (p *TestPipeline) WriteRecord(c recordctx.Context) (pipeline.RecordStatus, error) {
	_, _ = fmt.Fprintf(p.logger, "write %s\n", p.Name)
	if p.WriteError != nil {
		return pipeline.RecordError, p.WriteError
	}
	return pipeline.RecordProcessed, nil
}

func (p *TestPipeline) Close(_ context.Context) error {
	_, _ = fmt.Fprintf(p.logger, "close %s\n", p.Name)
	return p.CloseError
}
