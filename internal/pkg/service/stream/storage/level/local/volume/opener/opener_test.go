package opener_test

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

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/opener"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
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
	for i := range 5 {
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
	tc.Opener = func(info volume.Spec) (*test.Volume, error) {
		return nil, errors.New("some open error")
	}

	// Create some volumes directories
	for i := range 5 {
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
	tc.Opener = func(info volume.Spec) (*test.Volume, error) {
		return test.NewTestVolume(volume.ID(fmt.Sprintf(`volume_%s_%s`, info.Type, info.Label)), "my-node", info), nil
	}

	// Create some volume directories
	require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "hdd", "1", "slices"), 0o750))
	require.NoError(t, os.WriteFile(filepath.Join(tc.VolumesPath, "hdd", "1", "drain"), nil, 0o640))
	require.NoError(t, os.WriteFile(filepath.Join(tc.VolumesPath, "hdd", "some-file"), nil, 0o640))
	require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "hdd", "2"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "hdd", "3"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "ssd", "1"), 0o750))
	require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "ssd", "2"), 0o750))

	volumes, err := tc.OpenVolumes()
	require.NoError(t, err)

	// Volume found
	vol, err := volumes.Volume("volume_hdd_3")
	require.NoError(t, err)
	assert.Equal(t, &test.Volume{NodeIDValue: "my-node", IDValue: "volume_hdd_3", TypeValue: "hdd", LabelValue: "3", PathValue: filepath.Join(tc.VolumesPath, "hdd", "3")}, vol)

	// Volume not found
	vol, err = volumes.Volume("foo")
	assert.Nil(t, vol)
	if assert.Error(t, err) {
		assert.Equal(t, `volume with ID "foo" not found`, err.Error())
	}

	// VolumeByType
	assert.Empty(t, volumes.VolumeByType("foo"))
	assert.Equal(t, []*test.Volume{
		{NodeIDValue: "my-node", IDValue: "volume_hdd_1", TypeValue: "hdd", LabelValue: "1", PathValue: filepath.Join(tc.VolumesPath, "hdd", "1")},
		{NodeIDValue: "my-node", IDValue: "volume_hdd_2", TypeValue: "hdd", LabelValue: "2", PathValue: filepath.Join(tc.VolumesPath, "hdd", "2")},
		{NodeIDValue: "my-node", IDValue: "volume_hdd_3", TypeValue: "hdd", LabelValue: "3", PathValue: filepath.Join(tc.VolumesPath, "hdd", "3")},
	}, volumes.VolumeByType("hdd"))
	assert.Equal(t, []*test.Volume{
		{NodeIDValue: "my-node", IDValue: "volume_ssd_1", TypeValue: "ssd", LabelValue: "1", PathValue: filepath.Join(tc.VolumesPath, "ssd", "1")},
		{NodeIDValue: "my-node", IDValue: "volume_ssd_2", TypeValue: "ssd", LabelValue: "2", PathValue: filepath.Join(tc.VolumesPath, "ssd", "2")},
	}, volumes.VolumeByType("ssd"))

	// Count
	assert.Equal(t, 5, volumes.Count())

	// All
	assert.Equal(t, []*test.Volume{
		{NodeIDValue: "my-node", IDValue: "volume_hdd_1", TypeValue: "hdd", LabelValue: "1", PathValue: filepath.Join(tc.VolumesPath, "hdd", "1")},
		{NodeIDValue: "my-node", IDValue: "volume_hdd_2", TypeValue: "hdd", LabelValue: "2", PathValue: filepath.Join(tc.VolumesPath, "hdd", "2")},
		{NodeIDValue: "my-node", IDValue: "volume_hdd_3", TypeValue: "hdd", LabelValue: "3", PathValue: filepath.Join(tc.VolumesPath, "hdd", "3")},
		{NodeIDValue: "my-node", IDValue: "volume_ssd_1", TypeValue: "ssd", LabelValue: "1", PathValue: filepath.Join(tc.VolumesPath, "ssd", "1")},
		{NodeIDValue: "my-node", IDValue: "volume_ssd_2", TypeValue: "ssd", LabelValue: "2", PathValue: filepath.Join(tc.VolumesPath, "ssd", "2")},
	}, volumes.All())

	// Close - no error
	require.NoError(t, volumes.Close(t.Context()))
}

func TestOpenVolumes_CloseError(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)
	tc.Opener = func(info volume.Spec) (*test.Volume, error) {
		vol := test.NewTestVolume(volume.ID(fmt.Sprintf(`volume_%s_%s`, info.Type, info.Label)), "my-node", info)
		vol.CloseError = errors.New("some close error")
		return vol, nil
	}

	// Create some volumes directories
	for i := range 5 {
		require.NoError(t, os.MkdirAll(filepath.Join(tc.VolumesPath, "default", cast.ToString(i)), 0o750))
	}

	volumes, err := tc.OpenVolumes()
	require.NoError(t, err)
	assert.Len(t, volumes.All(), 5)

	// Close - all volumes are closed in parallel, errors are aggregated
	err = volumes.Close(t.Context())
	if assert.Error(t, err) {
		assert.Equal(t, strings.Repeat("- some close error\n", 5), err.Error()+"\n")
	}
}

type openTestCase struct {
	Logger      log.DebugLogger
	VolumesPath string
	Opener      opener.Opener[*test.Volume]
}

func newVolumesTestCase(t *testing.T) *openTestCase {
	t.Helper()
	return &openTestCase{
		Logger:      log.NewDebugLogger(),
		VolumesPath: t.TempDir(),
		Opener: func(info volume.Spec) (*test.Volume, error) {
			return test.NewTestVolume("my-volume", "my-node", info), nil
		},
	}
}

func (tc *openTestCase) OpenVolumes() (*volume.Collection[*test.Volume], error) {
	return opener.OpenVolumes[*test.Volume](context.Background(), tc.Logger, tc.VolumesPath, tc.Opener)
}
