package router_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/encoding/json"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	definitionRepo "github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/repository"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/mapping/recordctx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/router/closesync"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
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
	writerNode, _ := testnode.StartDiskWriterNode(t, ctx, logger, etcdCfg, volumesCount, nil)

	// Create coordinator, to check reported revisions
	svcScope, _ := dependencies.NewMockedServiceScope(t, ctx, commonDeps.WithEtcdConfig(etcdCfg))
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
	sourceScp, mock := testnode.StartSourceNode(t, ctx, logger, etcdCfg, nil)
	clk := mock.Clock()

	// Wait for connection between nodes
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"info","message":"disk writer client connected from \"%s\" to \"disk-writer\" - \"%s\""}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Fixtures
	sink, sinkCreateRev := createSink(t, ctx, clk, sourceScp.DefinitionRepository())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"sink.router"}`)
		logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.router"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Open pipeline, send some data
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()
	body := io.NopCloser(strings.NewReader(`{"foo":"bar"}`))
	req := (&http.Request{Body: body}).WithContext(reqCtx)
	result := sourceScp.SinkRouter().DispatchToSource(sink.SourceKey, recordctx.FromHTTP(clk.Now(), req))
	if !assert.Empty(t, result.ErrorName, result.Message) {
		t.Log(json.MustEncodeString(result, true))
	}
	require.Equal(t, http.StatusOK, result.StatusCode, result.Message)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"info","message":"opened sink pipeline to 2 slices","component":"storage.router"}
`)
	}, 5*time.Second, 10*time.Millisecond)
	waitForMinRevInUse(t, sinkCreateRev)

	// Rotate file/slices
	rotateResult := sourceScp.StorageRepository().File().Rotate(sink.SinkKey, clk.Now()).Do(ctx)
	require.NoError(t, rotateResult.Err())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"info","message":"updated sink pipeline, 2 opened slices, 2 closed slices","component":"storage.router"}
`)
	}, 5*time.Second, 10*time.Millisecond)
	waitForMinRevInUse(t, rotateResult.Header().Revision)

	// Disable sink - close files/slices
	disableResult := sourceScp.DefinitionRepository().Sink().Disable(sink.SinkKey, clk.Now(), test.ByUser(), "reason").Do(ctx)
	require.NoError(t, disableResult.Err())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"info","message":"closed sink pipeline to 2 slices:%s","component":"storage.router"}
`)
	}, 5*time.Second, 10*time.Millisecond)
	waitForMinRevInUse(t, disableResult.Header().Revision)

	// Shutdown the source node
	sourceScp.Process().Shutdown(ctx, errors.New("bye bye"))
	sourceScp.Process().WaitForShutdown()
	waitForMinRevInUse(t, closesync.NoSourceNode)

	// Shutdown the disk writer node
	writerNode.Process().Shutdown(ctx, errors.New("bye bye"))
	writerNode.Process().WaitForShutdown()
}

func TestRouter_ShutdownDiskWriterNodeFirst(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := log.NewDebugLogger()
	logger.ConnectTo(testhelper.VerboseStdout())
	etcdCfg := etcdhelper.TmpNamespace(t)

	// Start disk writer node
	volumesCount := 2
	writerNode, _ := testnode.StartDiskWriterNode(t, ctx, logger, etcdCfg, volumesCount, nil)

	// Wait for volumes registration
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"info","message":"disk writer listening on \"%s\""}`)
		logger.AssertJSONMessages(c, `{"level":"info","message":"registered \"2\" volumes"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Start a source node
	sourceScp, mock := testnode.StartSourceNode(t, ctx, logger, etcdCfg, nil)
	clk := mock.Clock()

	// Wait for connection between nodes
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"info","message":"disk writer client connected from \"%s\" to \"disk-writer\" - \"%s\""}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Fixtures
	sink, _ := createSink(t, ctx, clk, sourceScp.DefinitionRepository())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"sink.router"}`)
		logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.router"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Open pipeline, send some data
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()
	body := io.NopCloser(strings.NewReader(`{"foo":"bar"}`))
	req := (&http.Request{Body: body}).WithContext(reqCtx)
	result := sourceScp.SinkRouter().DispatchToSource(sink.SourceKey, recordctx.FromHTTP(clk.Now(), req))
	if !assert.Empty(t, result.ErrorName, result.Message) {
		t.Log(json.MustEncodeString(result, true))
	}
	require.Equal(t, http.StatusOK, result.StatusCode, result.Message)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"info","message":"opened sink pipeline to 2 slices","component":"storage.router"}
`)
	}, 5*time.Second, 10*time.Millisecond)

	// Shutdown the disk writer node, it notifies the source node
	writerNode.Process().Shutdown(ctx, errors.New("bye bye"))
	writerNode.Process().WaitForShutdown()
	logger.AssertJSONMessages(t, `
{"level":"info","message":"closing network file server","nodeId":"disk-writer","component":"storage.node.writer.rpc"}
{"level":"info","message":"waiting for close of 2 disk writers by source nodes","nodeId":"disk-writer","component":"storage.node.writer.rpc"}
{"level":"info","message":"all writers have been gracefully closed","nodeId":"disk-writer","component":"storage.node.writer.rpc"}
{"level":"info","message":"closing disk writer transport","nodeId":"disk-writer","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"waiting for streams done","nodeId":"disk-writer","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"closing 0 streams","nodeId":"disk-writer","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"closing 1 sessions","nodeId":"disk-writer","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"closed disk writer transport","nodeId":"disk-writer","component":"storage.node.writer.rpc.transport"}
`)
	logger.AssertJSONMessages(t, `
{"level":"info","message":"closed slice pipeline: remote server shutdown","nodeId":"source","volume.id":"my-volume-%d","component":"storage.router"}
{"level":"info","message":"closed slice pipeline: remote server shutdown","nodeId":"source","volume.id":"my-volume-%d","component":"storage.router"}
{"level":"debug","message":"closing sink pipeline to 0 slices: no slice pipeline","nodeId":"source","component":"storage.router"}
{"level":"info","message":"closed sink pipeline to 0 slices: no slice pipeline","nodeId":"source","component":"storage.router"}

`)

	// Shutdown the source node - all pipelines are already closed
	logger.Truncate()
	sourceScp.Process().Shutdown(ctx, errors.New("bye bye"))
	sourceScp.Process().WaitForShutdown()
	logger.AssertJSONMessages(t, `
{"level":"info","message":"closing storage router","nodeId":"source","component":"storage.router"}
{"level":"info","message":"closing 0 sink pipelines","nodeId":"source","component":"storage.router"}
{"level":"info","message":"closed storage router","nodeId":"source","component":"storage.router"}
{"level":"info","message":"closing sink router","nodeId":"source","component":"sink.router"}
{"level":"info","message":"closed sink router","nodeId":"source","component":"sink.router"}
{"level":"info","message":"stopping storage statistics collector","nodeId":"source","component":"statistics.collector"}
{"level":"info","message":"storage statistics stopped","nodeId":"source","component":"statistics.collector"}
{"level":"info","message":"closing connections","nodeId":"source","component":"storage.router.connections"}
{"level":"info","message":"closing disk writer client","nodeId":"source","component":"storage.router.connections.client.transport"}
{"level":"info","message":"closing 0 connections","nodeId":"source","component":"storage.router.connections.client.transport"}
{"level":"info","message":"closed disk writer client","nodeId":"source","component":"storage.router.connections.client.transport"}
{"level":"info","message":"closed connections","nodeId":"source","component":"storage.router.connections"}
`)
}

func TestRouter_ShutdownSourceNodeFirst(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	logger := log.NewDebugLogger()
	logger.ConnectTo(testhelper.VerboseStdout())
	etcdCfg := etcdhelper.TmpNamespace(t)

	// Start disk writer node
	volumesCount := 2
	writerNode, _ := testnode.StartDiskWriterNode(t, ctx, logger, etcdCfg, volumesCount, nil)

	// Wait for volumes registration
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"info","message":"disk writer listening on \"%s\""}`)
		logger.AssertJSONMessages(c, `{"level":"info","message":"registered \"2\" volumes"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Start a source node
	sourceScp, mock := testnode.StartSourceNode(t, ctx, logger, etcdCfg, nil)
	clk := mock.Clock()

	// Wait for connection between nodes
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"info","message":"disk writer client connected from \"%s\" to \"disk-writer\" - \"%s\""}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Fixtures
	sink, _ := createSink(t, ctx, clk, sourceScp.DefinitionRepository())
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"sink.router"}`)
		logger.AssertJSONMessages(c, `{"level":"debug","message":"watch stream mirror synced to revision %d","component":"storage.router"}`)
	}, 5*time.Second, 10*time.Millisecond)

	// Open pipeline, send some data
	reqCtx, reqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer reqCancel()
	body := io.NopCloser(strings.NewReader(`{"foo":"bar"}`))
	req := (&http.Request{Body: body}).WithContext(reqCtx)
	result := sourceScp.SinkRouter().DispatchToSource(sink.SourceKey, recordctx.FromHTTP(clk.Now(), req))
	if !assert.Empty(t, result.ErrorName, result.Message) {
		t.Log(json.MustEncodeString(result, true))
	}
	require.Equal(t, http.StatusOK, result.StatusCode, result.Message)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, `
{"level":"info","message":"opened sink pipeline to 2 slices","component":"storage.router"}
`)
	}, 5*time.Second, 10*time.Millisecond)

	// Shutdown the source node - it closes also pipelines in the disk writer node
	logger.Truncate()
	sourceScp.Process().Shutdown(ctx, errors.New("bye bye"))
	sourceScp.Process().WaitForShutdown()
	logger.AssertJSONMessages(t, `
{"level":"info","message":"closing storage router","nodeId":"source","component":"storage.router"}
{"level":"info","message":"closing 1 sink pipelines","nodeId":"source","component":"storage.router"}
{"level":"info","message":"closed slice pipeline: shutdown","nodeId":"source","volume.id":"my-volume-%d","component":"storage.router"}
{"level":"info","message":"closed slice pipeline: shutdown","nodeId":"source","volume.id":"my-volume-%d","component":"storage.router"}
{"level":"info","message":"closed sink pipeline to 2 slices: shutdown","nodeId":"source","component":"storage.router"}
{"level":"info","message":"closed storage router","nodeId":"source","component":"storage.router"}
{"level":"info","message":"closing sink router","nodeId":"source","component":"sink.router"}
{"level":"info","message":"closed sink router","nodeId":"source","component":"sink.router"}
{"level":"info","message":"stopping storage statistics collector","nodeId":"source","component":"statistics.collector"}
{"level":"info","message":"storage statistics stopped","nodeId":"source","component":"statistics.collector"}
{"level":"info","message":"closing connections","nodeId":"source","component":"storage.router.connections"}
{"level":"info","message":"closing disk writer client","nodeId":"source","component":"storage.router.connections.client.transport"}
{"level":"info","message":"closing 1 connections","nodeId":"source","component":"storage.router.connections.client.transport"}
{"level":"info","message":"closed disk writer client","nodeId":"source","component":"storage.router.connections.client.transport"}
{"level":"info","message":"closed connections","nodeId":"source","component":"storage.router.connections"}
`)
	logger.AssertJSONMessages(t, `
{"level":"debug","message":"closed disk writer","nodeId":"disk-writer","volume.id":"my-volume-%d","component":"storage.node.writer.volumes.volume"}
{"level":"debug","message":"closed disk writer","nodeId":"disk-writer","volume.id":"my-volume-%d","component":"storage.node.writer.volumes.volume"}
	`)

	// Shutdown the disk writer node - all pipelines are already closed
	writerNode.Process().Shutdown(ctx, errors.New("bye bye"))
	writerNode.Process().WaitForShutdown()
	logger.AssertJSONMessages(t, `
{"level":"info","message":"closing network file server","nodeId":"disk-writer","component":"storage.node.writer.rpc"}
{"level":"info","message":"waiting for close of 0 disk writers by source nodes","nodeId":"disk-writer","component":"storage.node.writer.rpc"}
{"level":"info","message":"all writers have been gracefully closed","nodeId":"disk-writer","component":"storage.node.writer.rpc"}
{"level":"info","message":"closing disk writer transport","nodeId":"disk-writer","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"waiting for streams done","nodeId":"disk-writer","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"closing 0 streams","nodeId":"disk-writer","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"closing 0 sessions","nodeId":"disk-writer","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"closed disk writer transport","nodeId":"disk-writer","component":"storage.node.writer.rpc.transport"}
	`)
}

func createSink(t *testing.T, ctx context.Context, clk clock.Clock, r *definitionRepo.Repository) (definition.Sink, int64) {
	t.Helper()

	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	sink := dummy.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	sink.Config = testconfig.LocalVolumeConfig(2, []string{"hdd"})
	require.NoError(t, r.Branch().Create(&branch, clk.Now(), test.ByUser()).Do(ctx).Err())
	require.NoError(t, r.Source().Create(&source, clk.Now(), test.ByUser(), "create").Do(ctx).Err())
	sinkResult := r.Sink().Create(&sink, clk.Now(), test.ByUser(), "create").Do(ctx)
	require.NoError(t, sinkResult.Err())
	return sink, sinkResult.Header().Revision
}
