package keboola_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/keboola/go-client/pkg/keboola"
	utilsproject "github.com/keboola/go-utils/pkg/testproject"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcd "go.etcd.io/etcd/client/v3"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/rollback"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/api"
	genApi "github.com/keboola/keboola-as-code/internal/pkg/service/stream/api/gen/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/source/type/httpsource"
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

	etcdConfig etcdclient.Config
	etcdClient *etcd.Client

	project   *testproject.Project
	projectID keboola.ProjectID
	branchID  keboola.BranchID

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

	sinkKey key.SinkKey
	tableID keboola.TableID
}

func setup(t *testing.T, ctx context.Context, modifyConfig func(cfg *config.Config), options ...utilsproject.Option) testState {
	t.Helper()

	ts := testState{}
	ts.logger = log.NewDebugLogger()
	ts.logger.ConnectInfoTo(testhelper.VerboseStdout())
	ts.httpClient = &http.Client{}

	ts.logSection(t, "setup")

	ts.setupProject(t, options...)
	ts.setupEtcd(t)
	ts.startNodes(t, ctx, modifyConfig)

	ts.logSection(t, "setup done")

	return ts
}

func (ts *testState) setupProject(t *testing.T, options ...utilsproject.Option) {
	t.Helper()

	ts.logSection(t, "obtaining testing project")
	ts.project = testproject.GetTestProjectForTest(t, "", options...)

	ts.logSection(t, "clearing testing project")
	ts.projectID = keboola.ProjectID(ts.project.ID())
	defaultBranch, err := ts.project.DefaultBranch()
	require.NoError(t, err)
	ts.branchID = defaultBranch.ID
}

func (ts *testState) setupEtcd(t *testing.T) {
	t.Helper()
	ts.etcdConfig = etcdhelper.TmpNamespace(t)
	ts.etcdClient = etcdhelper.ClientForTest(t, ts.etcdConfig)
}

func (ts *testState) startNodes(t *testing.T, ctx context.Context, modifyConfig func(cfg *config.Config)) {
	t.Helper()

	// API
	ts.logSection(t, "creating API scope")
	ts.apiScp, ts.apiMock = dependencies.NewMockedAPIScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "api"
			modifyConfig(c)
		},
		commonDeps.WithEtcdConfig(ts.etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(ts.project),
	)

	// Source
	ts.logSection(t, "creating 2 source scopes")
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
			modifyConfig(c)
		},
		commonDeps.WithEtcdConfig(ts.etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(ts.project),
	)
	ts.sourceScp2, ts.sourceMock2 = dependencies.NewMockedSourceScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "source2"
			c.Source.HTTP.Listen = fmt.Sprintf("0.0.0.0:%d", ts.sourcePort2)
			modifyConfig(c)
		},
		commonDeps.WithEtcdConfig(ts.etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(ts.project),
	)

	// Disk Writer
	ts.logSection(t, "creating 2 disk writer scopes")
	volumesPath1 := t.TempDir()
	volumesPath2 := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(volumesPath1, "hdd", "1"), 0o750)) //nolint: forbidigo
	require.NoError(t, os.MkdirAll(filepath.Join(volumesPath2, "hdd", "1"), 0o750)) //nolint: forbidigo
	// if you want static volume ID: require.NoError(t, os.WriteFile(filepath.Join(volumesPath, "hdd", "3", model.IDFile), []byte("HDD_3"), 0o640))
	ts.writerPort1 = netutils.FreePortForTest(t)
	ts.writerPort2 = netutils.FreePortForTest(t)
	ts.writerScp1, ts.writerMock1 = dependencies.NewMockedStorageWriterScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "writer1"
			c.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", ts.writerPort1)
			c.Storage.VolumesPath = volumesPath1
			modifyConfig(c)
		},
		commonDeps.WithEtcdConfig(ts.etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(ts.project),
	)
	ts.writerScp2, ts.writerMock2 = dependencies.NewMockedStorageWriterScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "writer2"
			c.Storage.Level.Local.Writer.Network.Listen = fmt.Sprintf("0.0.0.0:%d", ts.writerPort2)
			c.Storage.VolumesPath = volumesPath2
			modifyConfig(c)
		},
		commonDeps.WithEtcdConfig(ts.etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(ts.project),
	)

	// Disk Writer
	ts.logSection(t, "creating 2 disk reader scopes")
	ts.readerScp1, ts.readerMock1 = dependencies.NewMockedStorageReaderScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "reader1"
			c.Storage.VolumesPath = volumesPath1
			modifyConfig(c)
		},
		commonDeps.WithEtcdConfig(ts.etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(ts.project),
	)
	ts.readerScp2, ts.readerMock2 = dependencies.NewMockedStorageReaderScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "reader2"
			c.Storage.VolumesPath = volumesPath2
			modifyConfig(c)
		},
		commonDeps.WithEtcdConfig(ts.etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(ts.project),
	)

	// Coordinator
	ts.logSection(t, "creating 2 coordinator scopes")
	ts.coordinatorScp1, ts.coordinatorMock1 = dependencies.NewMockedCoordinatorScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "coordinator1"
			modifyConfig(c)
		},
		commonDeps.WithEtcdConfig(ts.etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(ts.project),
	)
	ts.coordinatorScp2, ts.coordinatorMock2 = dependencies.NewMockedCoordinatorScopeWithConfig(
		t,
		ctx,
		func(c *config.Config) {
			c.NodeID = "coordinator2"
			modifyConfig(c)
		},
		commonDeps.WithEtcdConfig(ts.etcdConfig),
		commonDeps.WithDebugLogger(ts.logger),
		commonDeps.WithTestProject(ts.project),
	)

	// Start nodes
	ts.logSection(t, "starting nodes")
	require.NoError(t, api.Start(ctx, ts.apiScp, ts.apiMock.TestConfig()))
	require.NoError(t, httpsource.Start(ctx, ts.sourceScp1, ts.sourceMock1.TestConfig().Source.HTTP))
	require.NoError(t, httpsource.Start(ctx, ts.sourceScp2, ts.sourceMock2.TestConfig().Source.HTTP))
	require.NoError(t, writernode.Start(ctx, ts.writerScp1, ts.writerMock1.TestConfig()))
	require.NoError(t, writernode.Start(ctx, ts.writerScp2, ts.writerMock2.TestConfig()))
	require.NoError(t, readernode.Start(ctx, ts.readerScp1, ts.readerMock1.TestConfig()))
	require.NoError(t, readernode.Start(ctx, ts.readerScp2, ts.readerMock2.TestConfig()))
	require.NoError(t, coordinator.Start(ctx, ts.coordinatorScp1, ts.coordinatorMock1.TestConfig()))
	require.NoError(t, coordinator.Start(ctx, ts.coordinatorScp2, ts.coordinatorMock2.TestConfig()))

	// Wait for sources
	ts.logSection(t, "waiting for HTTP sources")
	require.NoError(t, netutils.WaitForHTTP(fmt.Sprintf("http://localhost:%d/health-check", ts.sourcePort1), 5*time.Second))
	require.NoError(t, netutils.WaitForHTTP(fmt.Sprintf("http://localhost:%d/health-check", ts.sourcePort2), 5*time.Second))

	// Wait for volumes registration
	ts.logSection(t, "waiting for volumes registration")
	assert.EventuallyWithT(t, func(collect *assert.CollectT) {
		etcdhelper.AssertKeys(t, ts.etcdClient, []string{
			"runtime/closesync/source/node/source1",
			"runtime/closesync/source/node/source2",
			"storage/volume/writer/%s",
			"storage/volume/writer/%s",
		}, etcdhelper.WithIgnoredKeyPattern("^runtime/distribution/group/"))
	}, 5*time.Second, 10*time.Millisecond)
}

func (ts *testState) setupSink(t *testing.T, ctx context.Context) {
	t.Helper()
	var err error
	ts.logSection(t, "creating sink")
	apiCtx := context.WithValue(ctx, dependencies.KeboolaProjectAPICtxKey, ts.project.ProjectAPI())
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
	ts.sourceURL1, err = source.FormatHTTPSourceURL(fmt.Sprintf("http://localhost:%d", ts.sourcePort1))
	require.NoError(t, err)
	ts.sourceURL2, err = source.FormatHTTPSourceURL(fmt.Sprintf("http://localhost:%d", ts.sourcePort2))
	require.NoError(t, err)
	ts.logSection(t, "created sink")
	ts.sinkKey = sink.SinkKey
	ts.tableID = sink.Table.Keboola.TableID
}

func (ts *testState) setupSourceThroughAPI(t *testing.T, ctx context.Context, expectedStatusCode int) {
	t.Helper()

	sourceKey := key.SourceKey{SourceID: "my-source"}
	payload := &genApi.CreateSourcePayload{
		SourceID: &sourceKey.SourceID,
		Type:     "http",
		Name:     "testSource",
	}
	out, err := json.Marshal(payload)
	if !assert.NoError(t, err) {
		ts.logger.Errorf(ctx, "unable to marshal source create payload: %v", err)
		return
	}

	url := ts.apiScp.APIPublicURL()
	url.Path = fmt.Sprintf("/v1/branches/%s/sources", ts.branchID.String())
	req, err := http.NewRequestWithContext(ctx, "POST", url.String(), bytes.NewBuffer(out))
	require.NoError(t, err)
	req.Header.Add("X-StorageAPI-Token", ts.project.StorageAPIToken().Token)
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if !assert.NoError(t, err) {
		ts.logger.Errorf(ctx, "to create source: %v", err)
		return
	}

	// When we expect success, the API returns accepted instead of status OK
	if expectedStatusCode < 400 {
		expectedStatusCode = http.StatusAccepted
	}

	if assert.Equal(t, expectedStatusCode, resp.StatusCode) {
		return
	}

	out, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	task := make(map[string]any)
	err = json.Unmarshal(out, &task)
	require.NoError(t, err)

	urlString := task["url"].(string)
	req, err = http.NewRequestWithContext(ctx, "GET", urlString, nil)
	require.NoError(t, err)
	req.Header.Add("X-StorageAPI-Token", ts.project.StorageAPIToken().Token)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, expectedStatusCode, resp.StatusCode)
}

func (ts *testState) setupSinkThroughAPI(t *testing.T, ctx context.Context, expectedStatusCode int) {
	t.Helper()

	sourceKey := key.SourceKey{SourceID: "my-source"}
	sinkKey := key.SinkKey{SinkID: "my-sink"}
	payload := &genApi.CreateSinkPayload{
		SourceID: sourceKey.SourceID,
		SinkID:   &sinkKey.SinkID,
		Type:     definition.SinkTypeTable,
		Name:     "testSink",
	}
	out, err := json.Marshal(payload)
	if !assert.NoError(t, err) {
		ts.logger.Errorf(ctx, "unable to marshal sink create payload: %v", err)
		return
	}

	url := ts.apiScp.APIPublicURL()
	url.Path = fmt.Sprintf("/v1/branches/%s/sources/%s/sinks", ts.branchID.String(), sourceKey.SourceID.String())
	req, err := http.NewRequestWithContext(ctx, "POST", url.String(), bytes.NewBuffer(out))
	require.NoError(t, err)
	req.Header.Add("X-StorageAPI-Token", ts.project.StorageAPIToken().Token)
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if !assert.NoError(t, err) {
		ts.logger.Errorf(ctx, "to create sink: %v", err)
		return
	}

	// When we expect success, the API returns accepted instead of status OK
	if expectedStatusCode < 400 {
		expectedStatusCode = http.StatusAccepted
	}

	if assert.Equal(t, expectedStatusCode, resp.StatusCode) {
		return
	}

	out, err = io.ReadAll(resp.Body)
	require.NoError(t, err)
	task := make(map[string]any)
	err = json.Unmarshal(out, &task)
	require.NoError(t, err)

	urlString := task["url"].(string)
	req, err = http.NewRequestWithContext(ctx, "GET", urlString, nil)
	require.NoError(t, err)
	req.Header.Add("X-StorageAPI-Token", ts.project.StorageAPIToken().Token)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	assert.Equal(t, expectedStatusCode, resp.StatusCode)
}
