package connection_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/connection"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/writernode"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
)

func TestConnectionManager(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	etcdCfg := etcdhelper.TmpNamespace(t)

	// There are two disk writer nodes, before the source node is started
	w1 := startWriterNode(t, ctx, etcdCfg, "w1")
	w2 := startWriterNode(t, ctx, etcdCfg, "w2")
	waitForLog(t, w1.DebugLogger(), `{"level":"info","message":"disk writer listening on \"%s\"","component":"storage.node.writer.rpc.transport"}`)
	waitForLog(t, w2.DebugLogger(), `{"level":"info","message":"disk writer listening on \"%s\"","component":"storage.node.writer.rpc.transport"}`)

	// Start source node
	connManager, s := startSourceNode(t, ctx, etcdCfg, "s1")
	sourceLogger := s.DebugLogger()
	waitForLog(t, sourceLogger, `{"level":"info","message":"the list of volumes has changed, updating connections: \"%s\" - \"%s\"","component":"storage.router.connections"}`)
	waitForLog(t, sourceLogger, `{"level":"info","message":"disk writer client connected from \"%s\" to \"w1\" - \"l%s\"","component":"storage.router.connections.client.transport"}`)
	waitForLog(t, sourceLogger, `{"level":"info","message":"disk writer client connected from \"%s\" to \"w2\" - \"l%s\"","component":"storage.router.connections.client.transport"}`)
	waitForLog(t, w1.DebugLogger(), `{"level":"info","message":"accepted connection from \"%s\" to \"%s\"","component":"storage.node.writer.rpc.transport"}`)
	waitForLog(t, w2.DebugLogger(), `{"level":"info","message":"accepted connection from \"%s\" to \"%s\"","component":"storage.node.writer.rpc.transport"}`)
	sourceLogger.Truncate()
	assert.Equal(t, 2, connManager.ConnectionsCount())
	if conn, found := connManager.ConnectionToNode("w1"); assert.True(t, found) {
		assert.True(t, conn.IsConnected())
	}
	if conn, found := connManager.ConnectionToNode("w2"); assert.True(t, found) {
		assert.True(t, conn.IsConnected())
	}

	// Start another 2 writer nodes
	w3 := startWriterNode(t, ctx, etcdCfg, "w3")
	w4 := startWriterNode(t, ctx, etcdCfg, "w4")
	waitForLog(t, sourceLogger, `{"level":"info","message":"the list of volumes has changed, updating connections: \"%s\" - \"%s\"","component":"storage.router.connections"}`)
	waitForLog(t, sourceLogger, `{"level":"info","message":"disk writer client connected from \"%s\" to \"w3\" - \"l%s\"","component":"storage.router.connections.client.transport"}`)
	waitForLog(t, sourceLogger, `{"level":"info","message":"disk writer client connected from \"%s\" to \"w4\" - \"l%s\"","component":"storage.router.connections.client.transport"}`)
	waitForLog(t, w3.DebugLogger(), `{"level":"info","message":"accepted connection from \"%s\" to \"%s\"","component":"storage.node.writer.rpc.transport"}`)
	waitForLog(t, w4.DebugLogger(), `{"level":"info","message":"accepted connection from \"%s\" to \"%s\"","component":"storage.node.writer.rpc.transport"}`)
	sourceLogger.Truncate()
	assert.Equal(t, 4, connManager.ConnectionsCount())
	if conn, found := connManager.ConnectionToNode("w1"); assert.True(t, found) {
		assert.True(t, conn.IsConnected())
	}
	if conn, found := connManager.ConnectionToNode("w2"); assert.True(t, found) {
		assert.True(t, conn.IsConnected())
	}
	if conn, found := connManager.ConnectionToNode("w3"); assert.True(t, found) {
		assert.True(t, conn.IsConnected())
	}
	if conn, found := connManager.ConnectionToNode("w4"); assert.True(t, found) {
		assert.True(t, conn.IsConnected())
	}

	// Shutdown w1 and w3 (disconnect reason can be: 1. "session shutdown" OR 2. "EOF", so %s is used)
	w1.Process().Shutdown(ctx, errors.New("bye bye writer 1"))
	w1.Process().WaitForShutdown()
	w3.Process().Shutdown(ctx, errors.New("bye bye writer 3"))
	w3.Process().WaitForShutdown()
	waitForLog(t, sourceLogger, `{"level":"info","message":"the list of volumes has changed, updating connections: \"%s\" - \"%s\"","component":"storage.router.connections"}`)
	waitForLog(t, sourceLogger, `{"level":"info","message":"disk writer client disconnected from \"w1\" - \"localhost:%s\": %s","component":"storage.router.connections.client.transport"}`)
	waitForLog(t, sourceLogger, `{"level":"info","message":"disk writer client disconnected from \"w3\" - \"localhost:%s\": %s","component":"storage.router.connections.client.transport"}`)
	sourceLogger.Truncate()
	assert.Equal(t, 2, connManager.ConnectionsCount())
	_, found := connManager.ConnectionToNode("w1")
	assert.False(t, found)
	_, found = connManager.ConnectionToNode("w3")
	assert.False(t, found)
	if conn, found := connManager.ConnectionToVolume("w2-1"); assert.True(t, found) {
		assert.True(t, conn.IsConnected())
	}
	if conn, found := connManager.ConnectionToVolume("w4-2"); assert.True(t, found) {
		assert.True(t, conn.IsConnected())
	}

	// Shutdown source node - no warning/error is logged
	s.Process().Shutdown(ctx, errors.New("bye bye source"))
	s.Process().WaitForShutdown()
	waitForLog(t, sourceLogger, `{"level":"info","message":"disk writer client disconnected from \"w2\" - \"localhost:%s\": %s","component":"storage.router.connections.client.transport"}`)
	waitForLog(t, sourceLogger, `{"level":"info","message":"disk writer client disconnected from \"w4\" - \"localhost:%s\": %s","component":"storage.router.connections.client.transport"}`)
	sourceLogger.AssertJSONMessages(t, `{"level":"info","message":"exited"}`)
	waitForLog(t, w2.DebugLogger(), `{"level":"info","message":"closed connection from \"%s\"","component":"storage.node.writer.rpc.transport"}`)
	waitForLog(t, w4.DebugLogger(), `{"level":"info","message":"closed connection from \"%s\"","component":"storage.node.writer.rpc.transport"}`)
	sourceLogger.Truncate()

	// Shutdown w2 and w4
	w2.Process().Shutdown(ctx, errors.New("bye bye writer 2"))
	w2.Process().WaitForShutdown()
	w4.Process().Shutdown(ctx, errors.New("bye bye writer 4"))
	w4.Process().WaitForShutdown()

	// Check writer nodes logs
	expectedWriterLogs := `
{"level":"info","message":"searching for volumes in volumes path","component":"storage.node.writer.volumes"}
{"level":"info","message":"found \"2\" volumes","component":"storage.node.writer.volumes"}
{"level":"info","message":"starting storage writer node","component":"storage.node.writer"}
{"level":"info","message":"disk writer listening on \"%s\"","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"registered \"2\" volumes","component":"volumes.registry"}
{"level":"info","message":"exiting (bye bye writer %d)"}
{"level":"info","message":"closing disk writer transport","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"waiting %s for 0 streams","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"waiting for streams done","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"closing 0 streams","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"closing %d sessions","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"waiting for goroutines","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"closed disk writer transport","component":"storage.node.writer.rpc.transport"}
{"level":"info","message":"closing etcd connection","component":"etcd.client"}
{"level":"info","message":"closed etcd connection","component":"etcd.client"}
{"level":"info","message":"exited"}
`
	w1.DebugLogger().AssertJSONMessages(t, expectedWriterLogs)
	w2.DebugLogger().AssertJSONMessages(t, expectedWriterLogs)
	w3.DebugLogger().AssertJSONMessages(t, expectedWriterLogs)
	w4.DebugLogger().AssertJSONMessages(t, expectedWriterLogs)
}

func startSourceNode(t *testing.T, ctx context.Context, etcdCfg etcdclient.Config, nodeID string) (*connection.Manager, dependencies.Mocked) {
	t.Helper()

	d, m := dependencies.NewMockedSourceScopeWithConfig(
		t,
		ctx,
		func(cfg *config.Config) {
			cfg.NodeID = nodeID
		},
		commonDeps.WithEtcdConfig(etcdCfg),
	)

	return d.ConnectionManager(), m
}

func startWriterNode(t *testing.T, ctx context.Context, etcdCfg etcdclient.Config, nodeID string) dependencies.Mocked {
	t.Helper()

	volumesPath := t.TempDir()
	volumePath1 := filepath.Join(volumesPath, "hdd", "001")
	require.NoError(t, os.MkdirAll(volumePath1, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(volumePath1, volume.IDFile), []byte(fmt.Sprintf("%s-1", nodeID)), 0o600))
	volumePath2 := filepath.Join(volumesPath, "hdd", "002")
	require.NoError(t, os.MkdirAll(volumePath2, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(volumePath2, volume.IDFile), []byte(fmt.Sprintf("%s-2", nodeID)), 0o600))

	d, m := dependencies.NewMockedStorageWriterScopeWithConfig(
		t,
		ctx,
		func(cfg *config.Config) {
			cfg.NodeID = nodeID
			cfg.Storage.VolumesPath = volumesPath
			cfg.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", netutils.FreePortForTest(t))
		},
		commonDeps.WithEtcdConfig(etcdCfg),
	)

	require.NoError(t, writernode.Start(ctx, d, m.TestConfig()))

	return m
}

func waitForLog(t *testing.T, logger log.DebugLogger, expected string) {
	t.Helper()
	if !assert.EventuallyWithT(t, func(c *assert.CollectT) {
		logger.AssertJSONMessages(c, expected)
	}, 5*time.Second, 100*time.Millisecond) {
		t.Log(logger.AllMessages())
	}
}
