package volume_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestOpenVolumes_Empty(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)

	_, err := tc.OpenVolumes()
	if assert.Error(t, err) {
		assert.Equal(t, "no volume found", err.Error())
	}
}

func TestOpenVolumes_WalkError(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)
	tc.VolumesPath = "/missing/path"

	_, err := tc.OpenVolumes()
	assert.True(t, errors.Is(err, os.ErrNotExist))
}

func TestOpenVolumes_DuplicatedVolumeID(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)

	// Create some volumes directories
	for i := 0; i < 5; i++ {
		require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "default", cast.ToString(i)), 0o750))
	}

	_, err := tc.OpenVolumes()
	if assert.Error(t, err) {
		assert.Equal(t, `found 5 volumes with the ID "my-volume"`, err.Error())
	}
}

func TestOpenVolumes_OpenError(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)
	tc.Opener = func(info storage.VolumeSpec) (*test.Volume, error) {
		return nil, errors.New("some open error")
	}

	// Create some volumes directories
	for i := 0; i < 5; i++ {
		require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "default", cast.ToString(i)), 0o750))
	}

	_, err := tc.OpenVolumes()
	if assert.Error(t, err) {
		assert.Equal(t, strings.Repeat("- some open error\n", 5), err.Error()+"\n")
	}
}

func TestOpenAndCloseVolumes(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)
	tc.Opener = func(info storage.VolumeSpec) (*test.Volume, error) {
		return test.NewTestVolume(storage.VolumeID(fmt.Sprintf(`volume_%s_%s`, info.Type, info.Label)), "my-node", info), nil
	}

	// Create some volume directories
	require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "hdd", "1", "slices"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(tc.VolumesPath, "hdd", "1", "drain"), nil, 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(tc.VolumesPath, "hdd", "some-file"), nil, 0o640))
	require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "HDD", "2"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "hdd", "3"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "SSD", "1"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "ssd", "2"), 0o750))

	collection, err := tc.OpenVolumes()
	require.NoError(t, err)

	// Volume found
	vol, err := collection.Volume("volume_hdd_3")
	require.NoError(t, err)
	assert.Equal(t, &test.Volume{NodeIDValue: "my-node", IDValue: "volume_hdd_3", TypeValue: "hdd", LabelValue: "3", PathValue: filepath.Join(tc.VolumesPath, "hdd", "3")}, vol)

	// Volume not found
	vol, err = collection.Volume("foo")
	assert.Nil(t, vol)
	if assert.Error(t, err) {
		assert.Equal(t, `volume with ID "foo" not found`, err.Error())
	}

	// VolumeByType
	assert.Len(t, collection.VolumeByType("foo"), 0)
	assert.Equal(t, []*test.Volume{
		{NodeIDValue: "my-node", IDValue: "volume_hdd_1", TypeValue: "hdd", LabelValue: "1", PathValue: filepath.Join(tc.VolumesPath, "hdd", "1")},
		{NodeIDValue: "my-node", IDValue: "volume_hdd_2", TypeValue: "hdd", LabelValue: "2", PathValue: filepath.Join(tc.VolumesPath, "HDD", "2")},
		{NodeIDValue: "my-node", IDValue: "volume_hdd_3", TypeValue: "hdd", LabelValue: "3", PathValue: filepath.Join(tc.VolumesPath, "hdd", "3")},
	}, collection.VolumeByType("hdd"))
	assert.Equal(t, []*test.Volume{
		{NodeIDValue: "my-node", IDValue: "volume_ssd_1", TypeValue: "ssd", LabelValue: "1", PathValue: filepath.Join(tc.VolumesPath, "SSD", "1")},
		{NodeIDValue: "my-node", IDValue: "volume_ssd_2", TypeValue: "ssd", LabelValue: "2", PathValue: filepath.Join(tc.VolumesPath, "ssd", "2")},
	}, collection.VolumeByType("ssd"))

	// Count
	assert.Equal(t, 5, collection.Count())

	// All
	assert.Equal(t, []*test.Volume{
		{NodeIDValue: "my-node", IDValue: "volume_hdd_1", TypeValue: "hdd", LabelValue: "1", PathValue: filepath.Join(tc.VolumesPath, "hdd", "1")},
		{NodeIDValue: "my-node", IDValue: "volume_hdd_2", TypeValue: "hdd", LabelValue: "2", PathValue: filepath.Join(tc.VolumesPath, "HDD", "2")},
		{NodeIDValue: "my-node", IDValue: "volume_hdd_3", TypeValue: "hdd", LabelValue: "3", PathValue: filepath.Join(tc.VolumesPath, "hdd", "3")},
		{NodeIDValue: "my-node", IDValue: "volume_ssd_1", TypeValue: "ssd", LabelValue: "1", PathValue: filepath.Join(tc.VolumesPath, "SSD", "1")},
		{NodeIDValue: "my-node", IDValue: "volume_ssd_2", TypeValue: "ssd", LabelValue: "2", PathValue: filepath.Join(tc.VolumesPath, "ssd", "2")},
	}, collection.All())

	// Close - no error
	require.NoError(t, collection.Close(context.Background()))
}

func TestOpenVolumes_CloseError(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)
	tc.Opener = func(info storage.VolumeSpec) (*test.Volume, error) {
		vol := test.NewTestVolume(storage.VolumeID(fmt.Sprintf(`volume_%s_%s`, info.Type, info.Label)), "my-node", info)
		vol.CloseError = errors.New("some close error")
		return vol, nil
	}

	// Create some volumes directories
	for i := 0; i < 5; i++ {
		require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "default", cast.ToString(i)), 0o750))
	}

	collection, err := tc.OpenVolumes()
	require.NoError(t, err)
	assert.Len(t, collection.All(), 5)

	// Close - all volumes are closed in parallel, errors are aggregated
	err = collection.Close(context.Background())
	if assert.Error(t, err) {
		assert.Equal(t, strings.Repeat("- some close error\n", 5), err.Error()+"\n")
	}
}
