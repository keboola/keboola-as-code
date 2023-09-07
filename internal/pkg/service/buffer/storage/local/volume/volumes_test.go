package volume_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/cast"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/log"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/local/volume"
	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func TestVolumes_Empty(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)

	// Open volumes
	_, err := tc.New()
	if assert.Error(t, err) {
		assert.Equal(t, "no volume found", err.Error())
	}
}

func TestVolumes_WalkError(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)
	tc.Path = "/missing/path"

	// Open volumes
	_, err := tc.New()
	assert.True(t, errors.Is(err, os.ErrNotExist))
}

func TestVolumes_DuplicatedVolumeID(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)

	// Create some volumes directories
	for i := 0; i < 5; i++ {
		assert.NoError(t, os.MkdirAll(filepath.Join(tc.Path, "default", cast.ToString(i)), 0o750))
	}

	// Open volumes
	_, err := tc.New()
	if assert.Error(t, err) {
		assert.Equal(t, `found 5 volumes with the ID "abcdef"`, err.Error())
	}
}

func TestVolumes_Open_Error(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)
	tc.Opener = func(info volume.Info) (*testVolume, error) {
		return nil, errors.New("some open error")
	}

	// Create some volumes directories
	for i := 0; i < 5; i++ {
		assert.NoError(t, os.MkdirAll(filepath.Join(tc.Path, "default", cast.ToString(i)), 0o750))
	}

	// Open volumes
	_, err := tc.New()
	if assert.Error(t, err) {
		assert.Equal(t, strings.Repeat("- some open error\n", 5), err.Error()+"\n")
	}
}

func TestVolumes_Close_Error(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)
	tc.Opener = func(info volume.Info) (*testVolume, error) {
		vol := newTestVolume(storage.VolumeID("volume"+info.Type()+info.Label()), info)
		vol.CloseError = errors.New("some close error")
		return vol, nil
	}

	// Create some volumes directories
	for i := 0; i < 5; i++ {
		assert.NoError(t, os.MkdirAll(filepath.Join(tc.Path, "default", cast.ToString(i)), 0o750))
	}

	// Open volumes
	volumes, err := tc.New()
	assert.NoError(t, err)
	assert.Len(t, volumes.All(), 5)

	// Close - all volumes are closed in parallel, errors are aggregated
	err = volumes.Close()
	if assert.Error(t, err) {
		assert.Equal(t, strings.Repeat("- some close error\n", 5), err.Error()+"\n")
	}
}

func TestVolumes_Ok(t *testing.T) {
	t.Parallel()
	tc := newVolumesTestCase(t)
	tc.Opener = func(info volume.Info) (*testVolume, error) {
		return newTestVolume(storage.VolumeID("volume"+info.Type()+info.Label()), info), nil
	}

	// Create some volume directories
	assert.NoError(t, os.MkdirAll(filepath.Join(tc.Path, "hdd", "1", "slices"), 0o750))
	assert.NoError(t, os.WriteFile(filepath.Join(tc.Path, "hdd", "1", "drain"), nil, 0o640))
	assert.NoError(t, os.WriteFile(filepath.Join(tc.Path, "hdd", "some-file"), nil, 0o640))
	assert.NoError(t, os.MkdirAll(filepath.Join(tc.Path, "HDD", "2"), 0o750))
	assert.NoError(t, os.MkdirAll(filepath.Join(tc.Path, "hdd", "3"), 0o750))
	assert.NoError(t, os.MkdirAll(filepath.Join(tc.Path, "SSD", "1"), 0o750))
	assert.NoError(t, os.MkdirAll(filepath.Join(tc.Path, "ssd", "2"), 0o750))

	// Open volumes
	volumes, err := tc.New()
	assert.NoError(t, err)

	// Volume found
	vol, err := volumes.Volume("volumehdd3")
	expectedVol := &testVolume{IDValue: "volumehdd3", TypeValue: "hdd", LabelValue: "3", PathValue: filepath.Join(tc.Path, "hdd", "3")}
	assert.Equal(t, expectedVol, vol)
	assert.NoError(t, err)

	// Volume not found
	vol, err = volumes.Volume("foo")
	assert.Nil(t, vol)
	if assert.Error(t, err) {
		assert.Equal(t, `volume with ID "foo" not found`, err.Error())
	}

	// VolumeByType
	assert.Len(t, volumes.VolumeByType("foo"), 0)
	assert.Equal(t, []*testVolume{
		{IDValue: "volumehdd1", TypeValue: "hdd", LabelValue: "1", PathValue: filepath.Join(tc.Path, "hdd", "1")},
		{IDValue: "volumehdd2", TypeValue: "hdd", LabelValue: "2", PathValue: filepath.Join(tc.Path, "HDD", "2")},
		{IDValue: "volumehdd3", TypeValue: "hdd", LabelValue: "3", PathValue: filepath.Join(tc.Path, "hdd", "3")},
	}, volumes.VolumeByType("hdd"))
	assert.Equal(t, []*testVolume{
		{IDValue: "volumessd1", TypeValue: "ssd", LabelValue: "1", PathValue: filepath.Join(tc.Path, "SSD", "1")},
		{IDValue: "volumessd2", TypeValue: "ssd", LabelValue: "2", PathValue: filepath.Join(tc.Path, "ssd", "2")},
	}, volumes.VolumeByType("ssd"))

	// All
	assert.Equal(t, []*testVolume{
		{IDValue: "volumehdd1", TypeValue: "hdd", LabelValue: "1", PathValue: filepath.Join(tc.Path, "hdd", "1")},
		{IDValue: "volumehdd2", TypeValue: "hdd", LabelValue: "2", PathValue: filepath.Join(tc.Path, "HDD", "2")},
		{IDValue: "volumehdd3", TypeValue: "hdd", LabelValue: "3", PathValue: filepath.Join(tc.Path, "hdd", "3")},
		{IDValue: "volumessd1", TypeValue: "ssd", LabelValue: "1", PathValue: filepath.Join(tc.Path, "SSD", "1")},
		{IDValue: "volumessd2", TypeValue: "ssd", LabelValue: "2", PathValue: filepath.Join(tc.Path, "ssd", "2")},
	}, volumes.All())

	// Close - no error
	assert.NoError(t, volumes.Close())
}

type testVolume struct {
	PathValue  string
	TypeValue  string
	LabelValue string
	IDValue    storage.VolumeID
	CloseError error
}

func newTestVolume(id storage.VolumeID, info volume.Info) *testVolume {
	return &testVolume{
		PathValue:  info.Path(),
		TypeValue:  info.Type(),
		LabelValue: info.Label(),
		IDValue:    id,
	}
}

func (v *testVolume) Path() string {
	return v.PathValue
}

func (v *testVolume) Type() string {
	return v.TypeValue
}

func (v *testVolume) Label() string {
	return v.LabelValue
}

func (v *testVolume) ID() storage.VolumeID {
	return v.IDValue
}

func (v *testVolume) Close() error {
	return v.CloseError
}

type volumesTestCase struct {
	Logger log.DebugLogger
	Path   string
	Opener volume.Opener[*testVolume]
}

func newVolumesTestCase(t *testing.T) *volumesTestCase {
	t.Helper()
	return &volumesTestCase{
		Logger: log.NewDebugLogger(),
		Path:   t.TempDir(),
		Opener: func(info volume.Info) (*testVolume, error) {
			return newTestVolume("abcdef", info), nil
		},
	}
}

func (tc *volumesTestCase) New() (*volume.Volumes[*testVolume], error) {
	return volume.OpenVolumes(tc.Logger, tc.Path, tc.Opener)
}
