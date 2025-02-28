package volume_test

import (
	"context"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/v3/concurrency"

	deps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRepository_Volume(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	clk := clockwork.NewFakeClockAt(utctime.MustParse("2000-01-03T01:00:00.000Z").Time())

	// Get services
	d, mocked := dependencies.NewMockedStorageScope(t, ctx, deps.WithClock(clk))
	client := mocked.TestEtcdClient()
	r := d.StorageRepository().Volume()

	// Create session
	session, err := concurrency.NewSession(client)
	require.NoError(t, err)

	// Fixtures
	volume1 := volume.Metadata{
		ID: volume.ID("my-volume-1"),
		Spec: volume.Spec{
			NodeID:      "writer-node",
			NodeAddress: "localhost:1234",
			Path:        "hdd/001",
			Type:        "hdd",
			Label:       "001",
		},
	}
	volume2 := volume.Metadata{
		ID: volume.ID("my-volume-2"),
		Spec: volume.Spec{
			NodeID:      "writer-node",
			NodeAddress: "localhost:1234",
			Path:        "hdd/002",
			Type:        "hdd",
			Label:       "002",
		},
	}
	volume3 := volume.Metadata{
		ID: volume.ID("my-volume-3"),
		Spec: volume.Spec{
			NodeID:      "reader-node",
			NodeAddress: "localhost:1234",
			Path:        "hdd/003",
			Type:        "hdd",
			Label:       "003",
		},
	}
	volume4 := volume.Metadata{
		ID: volume.ID("my-volume-4"),
		Spec: volume.Spec{
			NodeID:      "reader-node",
			NodeAddress: "localhost:1234",
			Path:        "hdd/004",
			Type:        "hdd",
			Label:       "004",
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
		assert.Equal(t, []volume.Metadata{volume1, volume2}, result)

		result, err = r.ListReaderVolumes().Do(ctx).All()
		require.NoError(t, err)
		assert.Equal(t, []volume.Metadata{volume3, volume4}, result)
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
  "nodeAddress": "localhost:1234",
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
  "nodeAddress": "localhost:1234",
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
  "nodeAddress": "localhost:1234",
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
  "nodeAddress": "localhost:1234",
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
