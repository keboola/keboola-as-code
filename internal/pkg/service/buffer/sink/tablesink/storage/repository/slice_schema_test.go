package repository

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

func TestSliceSchema(t *testing.T) {
	t.Parallel()
	s := newSliceSchema(serde.NewJSON(serde.NoValidation))

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
			s.InLevel(storage.LevelLocal).Prefix(),
			"storage/slice/level/local/",
		},
		{
			s.InLevel(storage.LevelStaging).Prefix(),
			"storage/slice/level/staging/",
		},
		{
			s.InLevel(storage.LevelTarget).Prefix(),
			"storage/slice/level/target/",
		},
		{
			s.InLevel(storage.LevelLocal).InObject(sliceKey.ProjectID).Prefix(),
			"storage/slice/level/local/123/",
		},
		{
			s.InLevel(storage.LevelLocal).InProject(sliceKey.ProjectID).Prefix(),
			"storage/slice/level/local/123/",
		},
		{
			s.InLevel(storage.LevelLocal).InBranch(sliceKey.BranchKey).Prefix(),
			"storage/slice/level/local/123/456/",
		},
		{
			s.InLevel(storage.LevelLocal).InSource(sliceKey.SourceKey).Prefix(),
			"storage/slice/level/local/123/456/my-source/",
		},
		{
			s.InLevel(storage.LevelLocal).InSink(sliceKey.SinkKey).Prefix(),
			"storage/slice/level/local/123/456/my-source/my-sink/",
		},
		{
			s.InLevel(storage.LevelLocal).InFile(sliceKey.FileKey).Prefix(),
			"storage/slice/level/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/",
		},
		{
			s.InLevel(storage.LevelLocal).ByKey(sliceKey).Key(),
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
		s.InLevel(storage.LevelLocal).InObject(storage.SliceKey{})
	})
}
