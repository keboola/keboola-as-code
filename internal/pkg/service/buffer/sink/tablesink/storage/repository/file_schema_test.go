package repository

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

func TestFileSchema(t *testing.T) {
	t.Parallel()
	s := newFileSchema(serde.NewJSON(serde.NoValidation))

	fileKey := test.NewFileKey()

	cases := []struct{ actual, expected string }{
		{
			s.Prefix(),
			"storage/file/",
		},
		{
			s.AllLevels().Prefix(),
			"storage/file/all/",
		},
		{
			s.InLevel(storage.LevelLocal).Prefix(),
			"storage/file/level/local/",
		},
		{
			s.InLevel(storage.LevelStaging).Prefix(),
			"storage/file/level/staging/",
		},
		{
			s.InLevel(storage.LevelTarget).Prefix(),
			"storage/file/level/target/",
		},
		{
			s.InLevel(storage.LevelLocal).InObject(fileKey.ProjectID).Prefix(),
			"storage/file/level/local/123/",
		},
		{
			s.InLevel(storage.LevelLocal).InProject(fileKey.ProjectID).Prefix(),
			"storage/file/level/local/123/",
		},
		{
			s.InLevel(storage.LevelLocal).InBranch(fileKey.BranchKey).Prefix(),
			"storage/file/level/local/123/456/",
		},
		{
			s.InLevel(storage.LevelLocal).InSource(fileKey.SourceKey).Prefix(),
			"storage/file/level/local/123/456/my-source/",
		},
		{
			s.InLevel(storage.LevelLocal).InSink(fileKey.SinkKey).Prefix(),
			"storage/file/level/local/123/456/my-source/my-sink/",
		},
		{
			s.InLevel(storage.LevelLocal).ByKey(fileKey).Key(),
			"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z",
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
