package keboola_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/httpsource"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/readernode"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/writernode"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
)

type testState struct {
	logger           log.DebugLogger
	client           *etcd.Client
	apiScp           dependencies.APIScope
	apiMock          dependencies.Mocked
	sourcePort1      int
	sourceScp1       dependencies.SourceScope
	sourceMock1      dependencies.Mocked
	sourcePort2      int
	sourceScp2       dependencies.SourceScope
	sourceMock2      dependencies.Mocked
	writerPort1      int
	writerScp1       dependencies.StorageWriterScope
	writerMock1      dependencies.Mocked
	writerPort2      int
	writerScp2       dependencies.StorageWriterScope
	writerMock2      dependencies.Mocked
	readerScp1       dependencies.StorageReaderScope
	readerMock1      dependencies.Mocked
	readerScp2       dependencies.StorageReaderScope
	readerMock2      dependencies.Mocked
	coordinatorScp1  dependencies.CoordinatorScope
	coordinatorMock1 dependencies.Mocked
	coordinatorScp2  dependencies.CoordinatorScope
	coordinatorMock2 dependencies.Mocked
}

func (ts testState) teardown(ctx context.Context, t *testing.T) {
	t.Helper()
	type withProcess interface {
		Process() *servicectx.Process
	}
	scopes := []withProcess{
		ts.apiScp,
		ts.sourceScp1,
		ts.sourceScp2,
		ts.writerScp1,
		ts.writerScp2,
		ts.readerScp1,
		ts.readerScp2,
		ts.coordinatorScp1,
		ts.coordinatorScp2,
	}
	for _, s := range scopes {
		s.Process().Shutdown(ctx, errors.New("bye bye"))
	}

	for _, s := range scopes {
		s.Process().WaitForShutdown()
	}

}

func setup(ctx context.Context, t *testing.T) testState {
	t.Helper()
	ts := testState{}
	etcdConfig := etcdhelper.TmpNamespace(t)
	ts.logger = log.NewDebugLogger()
	ts.logger.ConnectTo(testhelper.VerboseStdout())
	ts.client = etcdhelper.ClientForTest(t, etcdConfig)

	ts.apiScp, ts.apiMock = dependencies.NewMockedAPIScopeWithConfig(
		t,
		func(c *config.Config) {
			c.NodeID = "api"
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
	)

	ts.sourcePort1 = netutils.FreePortForTest(t)
	ts.sourcePort2 = netutils.FreePortForTest(t)
	ts.sourceScp1, ts.sourceMock1 = dependencies.NewMockedSourceScopeWithConfig(
		t,
		func(c *config.Config) {
			c.NodeID = "source1"
			c.Source.HTTP.Listen = fmt.Sprintf("0.0.0.0:%d", ts.sourcePort1)
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
	)

	ts.sourceScp2, ts.sourceMock2 = dependencies.NewMockedSourceScopeWithConfig(
		t,
		func(c *config.Config) {
			c.NodeID = "source2"
			c.Source.HTTP.Listen = fmt.Sprintf("0.0.0.0:%d", ts.sourcePort2)
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
	)
	volumesPath1 := t.TempDir()
	volumesPath2 := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(volumesPath1, "hdd", "1"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(volumesPath2, "hdd", "1"), 0o750))
	//require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "3", model.IDFile), []byte("HDD_3"), 0o640))
	ts.writerPort1 = netutils.FreePortForTest(t)
	ts.writerPort2 = netutils.FreePortForTest(t)
	ts.writerScp1, ts.writerMock1 = dependencies.NewMockedStorageWriterScopeWithConfig(
		t,
		func(c *config.Config) {
			c.NodeID = "writer1"
			c.Hostname = "localhost"
			c.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", ts.writerPort1)
			c.Storage.VolumesPath = volumesPath1
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
	)
	ts.writerScp2, ts.writerMock2 = dependencies.NewMockedStorageWriterScopeWithConfig(
		t,
		func(c *config.Config) {
			c.NodeID = "writer2"
			c.Hostname = "localhost"
			c.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", ts.writerPort2)
			c.Storage.VolumesPath = volumesPath2
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
	)

	ts.readerScp1, ts.readerMock1 = dependencies.NewMockedStorageReaderScopeWithConfig(
		t,
		func(c *config.Config) {
			c.NodeID = "reader1"
			c.Hostname = "localhost"
			c.Storage.VolumesPath = volumesPath1
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
	)
	ts.readerScp2, ts.readerMock2 = dependencies.NewMockedStorageReaderScopeWithConfig(
		t,
		func(c *config.Config) {
			c.NodeID = "reader2"
			c.Hostname = "localhost"
			c.Storage.VolumesPath = volumesPath2
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
	)

	ts.coordinatorScp1, ts.coordinatorMock1 = dependencies.NewMockedCoordinatorScopeWithConfig(
		t,
		func(c *config.Config) {
			c.NodeID = "coordinator1"
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
	)
	ts.coordinatorScp2, ts.coordinatorMock2 = dependencies.NewMockedCoordinatorScopeWithConfig(
		t,
		func(c *config.Config) {
			c.NodeID = "coordinator2"
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
	)

	require.NoError(t, httpsource.Start(ctx, ts.sourceScp1, ts.sourceMock1.TestConfig().Source.HTTP))
	require.NoError(t, httpsource.Start(ctx, ts.sourceScp2, ts.sourceMock2.TestConfig().Source.HTTP))
	require.NoError(t, writernode.Start(ctx, ts.writerScp1, ts.writerMock1.TestConfig()))
	require.NoError(t, writernode.Start(ctx, ts.writerScp2, ts.writerMock2.TestConfig()))
	require.NoError(t, readernode.Start(ctx, ts.readerScp1, ts.readerMock1.TestConfig()))
	require.NoError(t, readernode.Start(ctx, ts.readerScp2, ts.readerMock2.TestConfig()))
	require.NoError(t, coordinator.Start(ctx, ts.coordinatorScp1, ts.coordinatorMock1.TestConfig()))
	require.NoError(t, coordinator.Start(ctx, ts.coordinatorScp2, ts.coordinatorMock2.TestConfig()))

	netutils.WaitForHTTP(fmt.Sprintf("localhost:%d", ts.sourcePort1), 5*time.Second)
	netutils.WaitForHTTP(fmt.Sprintf("localhost:%d", ts.sourcePort2), 5*time.Second)

	time.Sleep(2 * time.Second)
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		etcdhelper.AssertKeys(t, ts.client, []string{
			"runtime/closesync/source/node/source1",
			"runtime/closesync/source/node/source2",
			"runtime/distribution/group/operator.file.rotation/nodes/test-node",
			"runtime/distribution/group/operator.slice.rotation/nodes/test-node",
			"runtime/distribution/group/storage.router.sources.test-source/nodes/test-node",
			"storage/volume/writer/%s",
			"storage/volume/writer/%s",
		})
	}, 10*time.Second, 10*time.Millisecond)
	// Source
	// HttpSource(), request it direct, or sourceScope and dispatcher

	// WriterNode
	// How many volumes and nodes (2 nodes and 1 volume)

	// ReaderNode
	// 2 nodes and 1 volume

	// Coordinator
	// 2 Coordinator nodes to test work distribution
	return ts
}

func TestKeboolaBridgeWorkflow(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ts := setup(ctx, t)
	defer ts.teardown(ctx, t)

	ts.logger.AssertJSONMessages(t, "")
}
