package rpc_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdclient"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/network/rpc"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding"
	localModel "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/model"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/node/writernode"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/dummy"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/netutils"
)

func TestNetworkFile(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	etcdCfg := etcdhelper.TmpNamespace(t)

	// Create volume directory, with volume ID file
	volumeID := volume.ID("my-volume")
	volumesPath := t.TempDir()
	volumePath := filepath.Join(volumesPath, "hdd", "001")
	require.NoError(t, os.MkdirAll(volumePath, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(volumePath, volume.IDFile), []byte(volumeID), 0o600))

	// Start disk writer node
	writerNode := startDiskWriterNode(t, ctx, etcdCfg, "disk-writer", volumesPath)

	// Create resources in an API node
	apiScp, _ := dependencies.NewMockedAPIScope(t, ctx, commonDeps.WithEtcdConfig(etcdCfg))
	branchKey := key.BranchKey{ProjectID: 123, BranchID: 111}
	branch := test.NewBranch(branchKey)
	source := test.NewHTTPSource(key.SourceKey{BranchKey: branchKey, SourceID: "my-source"})
	source.HTTP.Secret = strings.Repeat("1", 48)
	sink := dummy.NewSinkWithLocalStorage(key.SinkKey{SourceKey: source.SourceKey, SinkID: "my-sink"})
	require.NoError(t, apiScp.DefinitionRepository().Branch().Create(&branch, apiScp.Clock().Now(), test.ByUser()).Do(ctx).Err())
	require.NoError(t, apiScp.DefinitionRepository().Source().Create(&source, apiScp.Clock().Now(), test.ByUser(), "create").Do(ctx).Err())
	require.NoError(t, apiScp.DefinitionRepository().Sink().Create(&sink, apiScp.Clock().Now(), test.ByUser(), "create").Do(ctx).Err())

	// Load created slice
	slices, err := apiScp.StorageRepository().Slice().ListIn(sink.SinkKey).Do(ctx).All()
	require.NoError(t, err)
	require.Len(t, slices, 1)
	slice := slices[0]

	// Open file on a source node
	file, sourceNode := openNetworkFile(t, ctx, etcdCfg, "source", slice.SliceKey, slice.LocalStorage)
	assert.True(t, file.IsReady())

	// Write
	n, err := file.Write(ctx, true, []byte("foo\n"))
	assert.Equal(t, 4, n)
	assert.NoError(t, err)
	n, err = file.Write(ctx, true, []byte("bar\n"))
	assert.Equal(t, 4, n)
	assert.NoError(t, err)

	// Sync
	assert.NoError(t, file.Sync(ctx))

	// Close
	assert.NoError(t, file.Close(ctx))

	// Shutdown nodes, the writer first
	writerNode.Process().Shutdown(ctx, errors.New("bye bye"))
	writerNode.Process().WaitForShutdown()
	sourceNode.Process().Shutdown(ctx, errors.New("bye bye"))
	sourceNode.Process().WaitForShutdown()

	// Check file content
	filePath := slice.LocalStorage.FileName(volumePath, "source")
	content, err := os.ReadFile(filePath)
	require.NoError(t, err)
	assert.Equal(t, "foo\nbar\n", string(content))
}

func startDiskWriterNode(t *testing.T, ctx context.Context, etcdCfg etcdclient.Config, nodeID string, volumesPath string) dependencies.Mocked {
	t.Helper()

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

func openNetworkFile(t *testing.T, ctx context.Context, etcdCfg etcdclient.Config, sourceNodeID string, sliceKey model.SliceKey, slice localModel.Slice) (encoding.NetworkOutput, dependencies.Mocked) {
	t.Helper()

	d, m := dependencies.NewMockedSourceScopeWithConfig(
		t,
		ctx,
		func(cfg *config.Config) {
			cfg.NodeID = sourceNodeID
		},
		commonDeps.WithEtcdConfig(etcdCfg),
	)

	// Obtain connection to the disk writer node
	conn, found := d.ConnectionManager().ConnectionToVolume(sliceKey.VolumeID)
	require.True(t, found)

	// Open network file
	onServerTermination := func() {}
	file, err := rpc.OpenNetworkFile(ctx, d.Logger(), sourceNodeID, conn, sliceKey, slice, onServerTermination)
	require.NoError(t, err)

	return file, m
}
