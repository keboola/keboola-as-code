package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
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
			s.InLevel(model.LevelLocal).Prefix(),
			"storage/slice/level/local/",
		},
		{
			s.InLevel(model.LevelStaging).Prefix(),
			"storage/slice/level/staging/",
		},
		{
			s.InLevel(model.LevelTarget).Prefix(),
			"storage/slice/level/target/",
		},
		{
			s.InLevel(model.LevelLocal).InObject(sliceKey.ProjectID).Prefix(),
			"storage/slice/level/local/123/",
		},
		{
			s.InLevel(model.LevelLocal).InProject(sliceKey.ProjectID).Prefix(),
			"storage/slice/level/local/123/",
		},
		{
			s.InLevel(model.LevelLocal).InBranch(sliceKey.BranchKey).Prefix(),
			"storage/slice/level/local/123/456/",
		},
		{
			s.InLevel(model.LevelLocal).InSource(sliceKey.SourceKey).Prefix(),
			"storage/slice/level/local/123/456/my-source/",
		},
		{
			s.InLevel(model.LevelLocal).InSink(sliceKey.SinkKey).Prefix(),
			"storage/slice/level/local/123/456/my-source/my-sink/",
		},
		{
			s.InLevel(model.LevelLocal).InFile(sliceKey.FileKey).Prefix(),
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/",
		},
		{
			s.InLevel(model.LevelLocal).InFileVolume(sliceKey.FileVolumeKey).Prefix(),
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/",
		},
		{
			s.InLevel(model.LevelLocal).ByKey(sliceKey).Key(),
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
		s.InLevel(model.LevelLocal).InObject(model.SliceKey{})
	})
}
