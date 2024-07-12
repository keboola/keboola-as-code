package registration_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	commonDeps "github.com/keboola/keboola-as-code/internal/pkg/service/common/dependencies"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/dependencies"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/registration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/etcdhelper"
)

func TestRegisterVolumes_RegisterWriterVolume(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	d, mock := dependencies.NewMockedLocalStorageScope(t, commonDeps.WithCtx(ctx))
	client := mock.TestEtcdClient()
	repo := d.StorageRepository().Volume()

	// Fixtures
	volumes := []*test.Volume{
		{
			IDValue:          "my-volume-4",
			NodeIDValue:      "node-1",
			NodeAddressValue: "localhost:1234",
			PathValue:        "type2/002",
			TypeValue:        "type2",
			LabelValue:       "002",
		},
		{
			IDValue:          "my-volume-2",
			NodeIDValue:      "node-1",
			NodeAddressValue: "localhost:1234",
			PathValue:        "type1/003",
			TypeValue:        "type1",
			LabelValue:       "003",
		},
		{
			IDValue:          "my-volume-1",
			NodeIDValue:      "node-1",
			NodeAddressValue: "localhost:1234",
			PathValue:        "type1/001",
			TypeValue:        "type1",
			LabelValue:       "001",
		},
		{
			IDValue:          "my-volume-3",
			NodeIDValue:      "node-1",
			NodeAddressValue: "localhost:1234",
			PathValue:        "type2/001",
			TypeValue:        "type2",
			LabelValue:       "001",
		},
	}

	// Create collection
	collection, err := volume.NewCollection(volumes)
	require.NoError(t, err)

	// Register volumes
	cfg := mock.TestConfig().Storage.Level.Local.Volume.Registration
	require.NoError(t, registration.RegisterVolumes(cfg, d, collection, repo.RegisterWriterVolume))

	// List
	result, err := repo.ListWriterVolumes().Do(ctx).All()
	require.NoError(t, err)
	assert.Equal(t, []volume.Metadata{
		{
			ID: "my-volume-1",
			Spec: volume.Spec{
				NodeID:      "node-1",
				NodeAddress: "localhost:1234",
				Path:        "type1/001",
				Type:        "type1",
				Label:       "001",
			},
		},
		{
			ID: "my-volume-2",
			Spec: volume.Spec{
				NodeID:      "node-1",
				NodeAddress: "localhost:1234",
				Path:        "type1/003",
				Type:        "type1",
				Label:       "003",
			},
		},
		{
			ID: "my-volume-3",
			Spec: volume.Spec{
				NodeID:      "node-1",
				NodeAddress: "localhost:1234",
				Path:        "type2/001",
				Type:        "type2",
				Label:       "001",
			},
		},
		{
			ID: "my-volume-4",
			Spec: volume.Spec{
				NodeID:      "node-1",
				NodeAddress: "localhost:1234",
				Path:        "type2/002",
				Type:        "type2",
				Label:       "002",
			},
		},
	}, result)

	// Un-register volumes on shutdown
	d.Process().Shutdown(ctx, errors.New("bye bye"))
	d.Process().WaitForShutdown()

	// Etcd database is empty (List cannot be used for assert, etcd client is closed by the shutdown)
	etcdhelper.AssertKVsString(t, client, "")

	// Close collection
	require.NoError(t, collection.Close(ctx))
}
