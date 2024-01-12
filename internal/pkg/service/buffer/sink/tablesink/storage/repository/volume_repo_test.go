package repository_test

import (
	"context"
	"testing"

	"github.com/benbjohnson/clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRepository_Volume(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	clk := clock.NewMock()
	clk.Set(utctime.MustParse("2000-01-03T01:00:00.000Z").Time())

	// Get services
	d, mocked := dependencies.NewMockedTableSinkScope(t, config.New(), deps.WithClock(clk))
	client := mocked.TestEtcdClient()
	r := d.StorageRepository().Volume()

	// Create session
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)

	// Fixtures
	volume1 := storage.VolumeMetadata{
		VolumeID: storage.VolumeID("my-volume-1"),
		VolumeSpec: storage.VolumeSpec{
			NodeID: "writer-node",
			Path:   "hdd/001",
			Type:   "hdd",
			Label:  "001",
		},
	}
	volume2 := storage.VolumeMetadata{
		VolumeID: storage.VolumeID("my-volume-2"),
		VolumeSpec: storage.VolumeSpec{
			NodeID: "writer-node",
			Path:   "hdd/002",
			Type:   "hdd",
			Label:  "002",
		},
	}
	volume3 := storage.VolumeMetadata{
		VolumeID: storage.VolumeID("my-volume-3"),
		VolumeSpec: storage.VolumeSpec{
			NodeID: "reader-node",
			Path:   "hdd/003",
			Type:   "hdd",
			Label:  "003",
		},
	}
	volume4 := storage.VolumeMetadata{
		VolumeID: storage.VolumeID("my-volume-4"),
		VolumeSpec: storage.VolumeSpec{
			NodeID: "reader-node",
			Path:   "hdd/004",
			Type:   "hdd",
			Label:  "004",
		},
	}

	// Empty
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Empty list
		result, err := r.ListWriterVolumes().Do(ctx).AllKVs()
		require.NoError(t, err)
		assert.Empty(t, result)

		result, err = r.ListReaderVolumes().Do(ctx).AllKVs()
		require.NoError(t, err)
		assert.Empty(t, result)
	}

	// Registration
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Register
		require.NoError(t, r.RegisterWriterVolume(volume1, session.Lease()).Do(ctx).Err())
		require.NoError(t, r.RegisterWriterVolume(volume2, session.Lease()).Do(ctx).Err())

		require.NoError(t, r.RegisterReaderVolume(volume3, session.Lease()).Do(ctx).Err())
		require.NoError(t, r.RegisterReaderVolume(volume4, session.Lease()).Do(ctx).Err())
	}
	{
		// List
		result, err := r.ListWriterVolumes().Do(ctx).All()
		require.NoError(t, err)
		assert.Equal(t, []storage.VolumeMetadata{volume1, volume2}, result)

		result, err = r.ListReaderVolumes().Do(ctx).All()
		require.NoError(t, err)
		assert.Equal(t, []storage.VolumeMetadata{volume3, volume4}, result)
	}
	{
		// Check etcd state
		etcdhelper.AssertKVsString(t, client, `
<<<<<
storage/volume/writer/my-volume-1 (lease)
-----
{
  "volumeId": "my-volume-1",
  "nodeId": "writer-node",
  "path": "hdd/001",
  "type": "hdd",
  "label": "001"
}
>>>>>

<<<<<
storage/volume/writer/my-volume-2 (lease)
-----
{
  "volumeId": "my-volume-2",
  "nodeId": "writer-node",
  "path": "hdd/002",
  "type": "hdd",
  "label": "002"
}
>>>>>

<<<<<
storage/volume/reader/my-volume-3 (lease)
-----
{
  "volumeId": "my-volume-3",
  "nodeId": "reader-node",
  "path": "hdd/003",
  "type": "hdd",
  "label": "003"
}
>>>>>

<<<<<
storage/volume/reader/my-volume-4 (lease)
-----
{
  "volumeId": "my-volume-4",
  "nodeId": "reader-node",
  "path": "hdd/004",
  "type": "hdd",
  "label": "004"
}
>>>>>
`)
	}

	// Un-registration
	// -----------------------------------------------------------------------------------------------------------------
	{
		// Un-register - close session, revoke lease
		require.NoError(t, session.Close())
	}
	{
		// Check etcd state
		etcdhelper.AssertKVsString(t, client, ``)
	}
}
