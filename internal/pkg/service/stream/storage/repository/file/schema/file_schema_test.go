package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/model"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestFileSchema(t *testing.T) {
	t.Parallel()
	s := New(serde.NewJSON(serde.NoValidation))

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
			s.InLevel(model.LevelLocal).Prefix(),
			"storage/file/level/local/",
		},
		{
			s.InLevel(model.LevelStaging).Prefix(),
			"storage/file/level/staging/",
		},
		{
			s.InLevel(model.LevelTarget).Prefix(),
			"storage/file/level/target/",
		},
		{
			s.InLevel(model.LevelLocal).InObject(fileKey.ProjectID).Prefix(),
			"storage/file/level/local/123/",
		},
		{
			s.InLevel(model.LevelLocal).InProject(fileKey.ProjectID).Prefix(),
			"storage/file/level/local/123/",
		},
		{
			s.InLevel(model.LevelLocal).InBranch(fileKey.BranchKey).Prefix(),
			"storage/file/level/local/123/456/",
		},
		{
			s.InLevel(model.LevelLocal).InSource(fileKey.SourceKey).Prefix(),
			"storage/file/level/local/123/456/my-source/",
		},
		{
			s.InLevel(model.LevelLocal).InSink(fileKey.SinkKey).Prefix(),
			"storage/file/level/local/123/456/my-source/my-sink/",
		},
		{
			s.InLevel(model.LevelLocal).ByKey(fileKey).Key(),
			"storage/file/level/local/123/456/my-source/my-sink/2000-01-01T01:00:00.000Z",
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
