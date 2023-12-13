package repository

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

type schemaTestCase struct{ actual, expected string }

func TestSchema(t *testing.T) {
	t.Parallel()
	s := newSchema(serde.NewJSON(serde.NoValidation))
	sliceKey := test.NewSliceKey()

	cases := []schemaTestCase{
		{
			s.Prefix(),
			"storage/stats/",
		},
		{
			s.InLevel(storage.LevelLocal).Prefix(),
			"storage/stats/local/",
		},
		{
			s.InLevel(storage.LevelStaging).Prefix(),
			"storage/stats/staging/",
		},
		{
			s.InLevel(storage.LevelTarget).Prefix(),
			"storage/stats/target/",
		},
		{
			s.InLevel(storage.LevelLocal).InObject(sliceKey.ProjectID).Prefix(),
			"storage/stats/local/123/",
		},
		{
			s.InLevel(storage.LevelLocal).InProject(sliceKey.ProjectID).Prefix(),
			"storage/stats/local/123/",
		},
		{
			s.InLevel(storage.LevelLocal).InBranch(sliceKey.BranchKey).Prefix(),
			"storage/stats/local/123/456/",
		},
		{
			s.InLevel(storage.LevelLocal).InSource(sliceKey.SourceKey).Prefix(),
			"storage/stats/local/123/456/my-source/",
		},
		{
			s.InLevel(storage.LevelLocal).InSource(sliceKey.SourceKey).Sum().Key(),
			"storage/stats/local/123/456/my-source/_sum",
		},
		{
			s.InLevel(storage.LevelLocal).InSink(sliceKey.SinkKey).Prefix(),
			"storage/stats/local/123/456/my-source/my-sink/",
		},
		{
			s.InLevel(storage.LevelLocal).InSink(sliceKey.SinkKey).Sum().Key(),
			"storage/stats/local/123/456/my-source/my-sink/_sum",
		},
		{
			s.InLevel(storage.LevelLocal).InFile(sliceKey.FileKey).Prefix(),
			"storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/",
		},
		{
			s.InLevel(storage.LevelLocal).InSlice(sliceKey).Key(),
			"storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z/value",
		},
		{
			s.InLevel(storage.LevelLocal).InParentOf(sliceKey.BranchKey).Prefix(),
			"storage/stats/local/123/",
		},
		{
			s.InLevel(storage.LevelLocal).InParentOf(sliceKey.SourceKey).Prefix(),
			"storage/stats/local/123/456/",
		},
		{
			s.InLevel(storage.LevelLocal).InParentOf(sliceKey.SinkKey).Prefix(),
			"storage/stats/local/123/456/my-source/",
		},
		{
			s.InLevel(storage.LevelLocal).InParentOf(sliceKey.FileKey).Prefix(),
			"storage/stats/local/123/456/my-source/my-sink/",
		},
		{
			s.InLevel(storage.LevelLocal).InParentOf(sliceKey).Prefix(),
			"storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}

	// Panics
	assert.Panics(t, func() {
		// Project is the top level
		s.InLevel(storage.LevelLocal).InParentOf(sliceKey.ProjectID)
	})
	assert.Panics(t, func() {
		s.InLevel("foo")
	})
}
