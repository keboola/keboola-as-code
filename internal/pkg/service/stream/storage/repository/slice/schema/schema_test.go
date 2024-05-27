package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestSliceSchema(t *testing.T) {
	t.Parallel()
	s := New(serde.NewJSON(serde.NoValidation))

	sliceKey := test.NewSliceKey()

	cases := []struct{ actual, expected string }{
		{
			s.Prefix(),
			"storage/slice/",
		},
		{
			s.AllLevels().Prefix(),
			"storage/slice/all/",
		},
		{
			s.InLevel(level.Local).Prefix(),
			"storage/slice/level/local/",
		},
		{
			s.InLevel(level.Staging).Prefix(),
			"storage/slice/level/staging/",
		},
		{
			s.InLevel(level.Target).Prefix(),
			"storage/slice/level/target/",
		},
		{
			s.InLevel(level.Local).InObject(sliceKey.ProjectID).Prefix(),
			"storage/slice/level/local/123/",
		},
		{
			s.InLevel(level.Local).InProject(sliceKey.ProjectID).Prefix(),
			"storage/slice/level/local/123/",
		},
		{
			s.InLevel(level.Local).InBranch(sliceKey.BranchKey).Prefix(),
			"storage/slice/level/local/123/456/",
		},
		{
			s.InLevel(level.Local).InSource(sliceKey.SourceKey).Prefix(),
			"storage/slice/level/local/123/456/my-source/",
		},
		{
			s.InLevel(level.Local).InSink(sliceKey.SinkKey).Prefix(),
			"storage/slice/level/local/123/456/my-source/my-sink/",
		},
		{
			s.InLevel(level.Local).InFile(sliceKey.FileKey).Prefix(),
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/",
		},
		{
			s.InLevel(level.Local).InFileVolume(sliceKey.FileVolumeKey).Prefix(),
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/",
		},
		{
			s.InLevel(level.Local).ByKey(sliceKey).Key(),
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}

	// Test panics
	assert.Panics(t, func() {
		// Unexpected storage level
		s.InLevel("foo")
	})
	assert.Panics(t, func() {
		// There is no file in slice level, slice is in file, not file in slice
		s.InLevel(level.Local).InObject(model.SliceKey{})
	})
}
