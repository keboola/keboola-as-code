package keboola_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/servicectx"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/httpsource"
	stagingConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	targetConfig "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/coordinator"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/readernode"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/writernode"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testconfig"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/testproject"
)

type testState struct {
	logger     log.DebugLogger
	httpClient *http.Client
	// TODO: rename
	client           *etcd.Client
	projectID        keboola.ProjectID
	branchID         keboola.BranchID
	apiScp           dependencies.APIScope
	apiMock          dependencies.Mocked
	sourcePort1      int
	sourceURL1       string
	sourceScp1       dependencies.SourceScope
	sourceMock1      dependencies.Mocked
	sourcePort2      int
	sourceURL2       string
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

type withProcess interface {
	Process() *servicectx.Process
}

func TestKeboolaBridgeWorkflow(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ts := setup(t, ctx)
	defer ts.teardown(t, ctx)

	// TODO: choose source scope
	for i := range 10 {
		req, err := http.NewRequest(http.MethodPost, ts.sourceURL1, strings.NewReader(fmt.Sprintf("foo%d", i)))
		require.NoError(t, err)
		resp, err := ts.httpClient.Do(req)
		assert.NoError(t, err)
		assert.NoError(t, resp.Body.Close())
	}
	ts.logger.AssertJSONMessages(t, "")
}

func setup(t *testing.T, ctx context.Context) testState {
	t.Helper()
	// TODO: add comment for empty path
	// TODO: logging of project not found
	project := testproject.GetTestProjectForTest(t, "")
	require.NoError(t, project.SetState("empty.json"))
	ts := testState{}
	ts.projectID = keboola.ProjectID(project.ID())
	defaultBranch, err := project.DefaultBranch()
	require.NoError(t, err)
	ts.branchID = defaultBranch.ID

	// ETCD + logger setup
	etcdConfig := etcdhelper.TmpNamespace(t)
	ts.logger = log.NewDebugLogger()
	ts.logger.ConnectTo(testhelper.VerboseStdout())
	ts.client = etcdhelper.ClientForTest(t, etcdConfig)
	ts.httpClient = &http.Client{}
	uploadTrigger := stagingConfig.UploadTrigger{
		Count:    10,
		Size:     1000 * datasize.MB,
		Interval: duration.From(30 * time.Minute),
	}
	importTrigger := targetConfig.ImportTrigger{
		Count:       20,
		Size:        1000 * datasize.MB,
		Interval:    duration.From(30 * time.Minute),
		SlicesCount: 100,
		// TODO: expiration test case
		Expiration: duration.From(30 * time.Minute),
	}

	ts.apiScp, ts.apiMock = dependencies.NewMockedAPIScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "api"
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(project),
	)

	ts.sourcePort1 = netutils.FreePortForTest(t)
	ts.sourcePort2 = netutils.FreePortForTest(t)
	ts.sourceURL1 = fmt.Sprintf("http://localhost:%d", ts.sourcePort1)
	ts.sourceURL2 = fmt.Sprintf("http://localhost:%d", ts.sourcePort2)
	ts.sourceScp1, ts.sourceMock1 = dependencies.NewMockedSourceScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "source1"
			c.Source.HTTP.Listen = fmt.Sprintf("0.0.0.0:%d", ts.sourcePort1)
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(project),
	)

	ts.sourceScp2, ts.sourceMock2 = dependencies.NewMockedSourceScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "source2"
			c.Source.HTTP.Listen = fmt.Sprintf("0.0.0.0:%d", ts.sourcePort2)
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(project),
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
		ctx,
		func(c *config.Config) {
			c.NodeID = "writer1"
			c.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", ts.writerPort1)
			c.Storage.VolumesPath = volumesPath1
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(project),
	)
	ts.writerScp2, ts.writerMock2 = dependencies.NewMockedStorageWriterScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "writer2"
			c.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", ts.writerPort2)
			c.Storage.VolumesPath = volumesPath2
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(project),
	)

	ts.readerScp1, ts.readerMock1 = dependencies.NewMockedStorageReaderScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "reader1"
			c.Storage.VolumesPath = volumesPath1
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(project),
	)
	ts.readerScp2, ts.readerMock2 = dependencies.NewMockedStorageReaderScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "reader2"
			c.Storage.VolumesPath = volumesPath2
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(project),
	)

	ts.coordinatorScp1, ts.coordinatorMock1 = dependencies.NewMockedCoordinatorScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "coordinator1"
			c.Storage.Level.Staging.Upload.Trigger = uploadTrigger
			c.Storage.Level.Target.Import.Trigger = importTrigger
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(project),
	)
	ts.coordinatorScp2, ts.coordinatorMock2 = dependencies.NewMockedCoordinatorScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "coordinator2"
			c.Storage.Level.Staging.Upload.Trigger = uploadTrigger
			c.Storage.Level.Target.Import.Trigger = importTrigger
		},
		commonDeps.WithEtcdConfig(etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(project),
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

	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		etcdhelper.AssertKeys(t, ts.client, []string{
			"runtime/closesync/source/node/source1",
			"runtime/closesync/source/node/source2",
			"runtime/distribution/group/operator.file.rotation/nodes/coordinator1",
			"runtime/distribution/group/operator.file.rotation/nodes/coordinator2",
			"runtime/distribution/group/operator.slice.rotation/nodes/coordinator1",
			"runtime/distribution/group/operator.slice.rotation/nodes/coordinator2",
			"runtime/distribution/group/operator.file.import/nodes/coordinator1",
			"runtime/distribution/group/operator.file.import/nodes/coordinator2",
			"runtime/distribution/group/storage.router.sources.test-source/nodes/source1",
			"runtime/distribution/group/storage.router.sources.test-source/nodes/source2",
			"storage/volume/writer/%s",
			"storage/volume/writer/%s",
		})
	}, 5*time.Second, 10*time.Millisecond)

	// Fixtures
	apiCtx := context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, project.ProjectAPI())
	apiCtx = rollback.ContextWith(apiCtx, rollback.New(ts.logger))
	branchKey := key.BranchKey{ProjectID: ts.projectID, BranchID: ts.branchID}
	branch := test.NewBranch(branchKey)
	sourceKey := key.SourceKey{BranchKey: branchKey, SourceID: "my-source"}
	source := test.NewHTTPSource(sourceKey)
	sink := test.NewKeboolaTableSink(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	sink.Config = testconfig.LocalVolumeConfig(2, []string{"hdd"})
	require.NoError(t, ts.apiScp.DefinitionRepository().Branch().Create(&branch, time.Now(), test.ByUser()).Do(apiCtx).Err())
	require.NoError(t, ts.apiScp.DefinitionRepository().Source().Create(&source, time.Now(), test.ByUser(), "create").Do(apiCtx).Err())
	require.NoError(t, ts.apiScp.DefinitionRepository().Sink().Create(&sink, time.Now(), test.ByUser(), "create").Do(apiCtx).Err())
	ts.sourceURL1 = formatHTTPSourceURL(t, fmt.Sprintf("http://localhost:%d", ts.sourcePort1), source)
	ts.sourceURL2 = formatHTTPSourceURL(t, fmt.Sprintf("http://localhost:%d", ts.sourcePort2), source)
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
func (ts testState) teardown(t *testing.T, ctx context.Context) {
	t.Helper()

	ts.shutdown(t, ctx, []withProcess{
		ts.apiScp,
		ts.sourceScp1,
		ts.sourceScp2,
	})

	ts.shutdown(t, ctx, []withProcess{
		ts.writerScp1,
		ts.writerScp2,
		ts.readerScp1,
		ts.readerScp2,
		ts.coordinatorScp1,
		ts.coordinatorScp2,
	})
}

func (ts testState) shutdown(t *testing.T, ctx context.Context, scopes []withProcess) {
	t.Helper()

	for _, s := range scopes {
		s.Process().Shutdown(ctx, errors.New("bye bye"))
	}

	for _, s := range scopes {
		s.Process().WaitForShutdown()
	}

}

func formatHTTPSourceURL(t *testing.T, baseURL string, entity definition.Source) string {
	u, err := url.Parse(baseURL)
	require.NoError(t, err)
	return u.
		JoinPath("stream", entity.ProjectID.String(), entity.SourceID.String(), entity.HTTP.Secret).
		String()
}
