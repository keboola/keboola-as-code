package writernode_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestStart_NoVolumeFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	d, mock := dependencies.NewMockedServiceScope(t, ctx)

	err := stream.StartComponents(ctx, d, mock.TestConfig(), stream.ComponentStorageWriter)
	require.Error(t, err)
	require.Equal(t, "no volume found", err.Error())
}

func TestStart_Ok(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	d, mock := dependencies.NewMockedServiceScope(t, ctx)
	client := mock.TestEtcdClient()

	// Create some volumes in volumes temp dir
	volumesPath := mock.TestConfig().Storage.VolumesPath
	volume1Path := filepath.Join(volumesPath, "hdd", "my-volume-1")
	volume2Path := filepath.Join(volumesPath, "hdd", "my-volume-2")
	require.NoError(t, os.MkdirAll(volume1Path, 0o700))
	require.NoError(t, os.MkdirAll(volume2Path, 0o700))

	// Start
	require.NoError(t, stream.StartComponents(ctx, d, mock.TestConfig(), stream.ComponentStorageWriter))

	// Each volume has a generated ID
	id1, err := os.ReadFile(filepath.Join(volume1Path, volume.IDFile))
	require.NoError(t, err)
	assert.NotEmpty(t, id1)
	id2, err := os.ReadFile(filepath.Join(volume2Path, volume.IDFile))
	require.NoError(t, err)
	assert.NotEmpty(t, id2)

	// Volumes have been registered
	etcdhelper.AssertKeys(t, client, []string{
		fmt.Sprintf("storage/volume/writer/%s", strings.TrimSpace(string(id1))),
		fmt.Sprintf("storage/volume/writer/%s", strings.TrimSpace(string(id2))),
	})

	// Shutdown
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// Volumes have been unregistered
	etcdhelper.AssertKeys(t, client, nil)

	// Logs
	mock.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"searching for volumes in volumes path","component":"storage.node.writer.volumes"}
{"level":"info","message":"found \"2\" volumes","component":"storage.node.writer.volumes"}
{"level":"info","message":"starting storage writer node","component":"storage.node.writer"}
{"level":"info","message":"creating etcd session","component":"volumes.registry.etcd.session"}
{"level":"info","message":"created etcd session","component":"volumes.registry.etcd.session"}
{"level":"info","message":"exiting (bye bye)"}
{"level":"info","message":"stopping volumes registration","component":"volumes.registry"}
{"level":"info","message":"closing etcd session: context canceled","component":"volumes.registry.etcd.session"}
{"level":"info","message":"closed etcd session","component":"volumes.registry.etcd.session"}
{"level":"info","message":"stopped volumes registration","component":"volumes.registry"}
{"level":"info","message":"closing volumes","component":"storage.node.writer.volumes"}
{"level":"info","message":"closed volumes","component":"storage.node.writer.volumes"}
{"level":"info","message":"closing volumes stream","component":"volume.repository"}
{"level":"info","message":"watch stream consumer closed: context canceled","component":"volume.repository"}
{"level":"info","message":"closed volumes stream","component":"volume.repository"}
{"level":"info","message":"closing etcd connection","component":"etcd.client"}
{"level":"info","message":"closed etcd connection","component":"etcd.client"}
{"level":"info","message":"exited"}
`)
}
