package readernode_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestStart_NoVolumeFound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	d, mock := dependencies.NewMockedServiceScope(t)

	// Start
	err := stream.StartComponents(ctx, d, mock.TestConfig(), stream.ComponentStorageReader)
	require.Error(t, err)
	require.Equal(t, "no volume found", err.Error())
}

func TestStart_Ok(t *testing.T) {
	t.Parallel()

	d, mock := dependencies.NewMockedServiceScope(t)

	// Create some volumes in volumes temp dir
	volumesPath := mock.TestConfig().Storage.VolumesPath
	volume1Path := filepath.Join(volumesPath, "hdd", "my-volume-1")
	volume2Path := filepath.Join(volumesPath, "hdd", "my-volume-2")
	require.NoError(t, os.MkdirAll(volume1Path, 0o700))
	require.NoError(t, os.MkdirAll(volume2Path, 0o700))

	// Reader node is waiting, if a volume has no ID file, generate IDs
	id1 := volume.GenerateID()
	id2 := volume.GenerateID()
	require.NoError(t, os.WriteFile(filepath.Join(volume1Path, volume.IDFile), []byte(id1), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(volume2Path, volume.IDFile), []byte(id2), 0o600))

	// Start
	ctx := context.Background()
	require.NoError(t, stream.StartComponents(ctx, d, mock.TestConfig(), stream.ComponentStorageReader))

	// Shutdown
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// Logs
	mock.DebugLogger().AssertJSONMessages(t, `
{"level":"info","message":"starting storage reader node","component":"storage.node.reader"}
{"level":"info","message":"searching for volumes in volumes path","volumes.path":"%s","component":"storage.node.reader"}
{"level":"info","message":"found \"2\" volumes","component":"storage.node.reader"}
{"level":"info","message":"exiting (bye bye)"}
{"level":"info","message":"closing volumes","component":"storage.node.reader"}
{"level":"info","message":"closing volumes stream","component":"volume.repository"}
{"level":"info","message":"closed volumes stream","component":"volume.repository"}
{"level":"info","message":"received shutdown request","component":"distribution.mutex.provider"}
{"level":"info","message":"closing etcd session: context canceled","component":"distribution.mutex.provider.etcd.session"}
{"level":"info","message":"closed etcd session","component":"distribution.mutex.provider.etcd.session"}
{"level":"info","message":"shutdown done","component":"distribution.mutex.provider"}
{"level":"info","message":"closing etcd connection","component":"etcd.client"}
{"level":"info","message":"closed etcd connection","component":"etcd.client"}
{"level":"info","message":"exited"}
`)
}
