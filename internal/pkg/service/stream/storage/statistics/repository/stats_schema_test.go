package repository

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
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
			s.InLevel(level.Local).Prefix(),
			"storage/stats/local/",
		},
		{
			s.InLevel(level.Staging).Prefix(),
			"storage/stats/staging/",
		},
		{
			s.InLevel(level.Target).Prefix(),
			"storage/stats/target/",
		},
		{
			s.InLevel(level.Local).InObject(sliceKey.ProjectID).Prefix(),
			"storage/stats/local/123/",
		},
		{
			s.InLevel(level.Local).InProject(sliceKey.ProjectID).Prefix(),
			"storage/stats/local/123/",
		},
		{
			s.InLevel(level.Local).InBranch(sliceKey.BranchKey).Prefix(),
			"storage/stats/local/123/456/",
		},
		{
			s.InLevel(level.Local).InSource(sliceKey.SourceKey).Prefix(),
			"storage/stats/local/123/456/my-source/",
		},
		{
			s.InLevel(level.Local).InSource(sliceKey.SourceKey).Sum().Key(),
			"storage/stats/local/123/456/my-source/_sum",
		},
		{
			s.InLevel(level.Local).InSink(sliceKey.SinkKey).Prefix(),
			"storage/stats/local/123/456/my-source/my-sink/",
		},
		{
			s.InLevel(level.Local).InSink(sliceKey.SinkKey).Sum().Key(),
			"storage/stats/local/123/456/my-source/my-sink/_sum",
		},
		{
			s.InLevel(level.Local).InFile(sliceKey.FileKey).Prefix(),
			"storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/",
		},
		{
			s.InLevel(level.Local).InSlice(sliceKey).Key(),
			"storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z/value",
		},
		{
			s.InLevel(level.Local).InParentOf(sliceKey.BranchKey).Prefix(),
			"storage/stats/local/123/",
		},
		{
			s.InLevel(level.Local).InParentOf(sliceKey.SourceKey).Prefix(),
			"storage/stats/local/123/456/",
		},
		{
			s.InLevel(level.Local).InParentOf(sliceKey.SinkKey).Prefix(),
			"storage/stats/local/123/456/my-source/",
		},
		{
			s.InLevel(level.Local).InParentOf(sliceKey.FileKey).Prefix(),
			"storage/stats/local/123/456/my-source/my-sink/",
		},
		{
			s.InLevel(level.Local).InParentOf(sliceKey).Prefix(),
			"storage/stats/local/123/456/my-source/my-sink/2000-01-01T19:00:00.000Z/",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}

	// Panics
	assert.Panics(t, func() {
		// Project is the top level
		s.InLevel(level.Local).InParentOf(sliceKey.ProjectID)
	})
	assert.Panics(t, func() {
		s.InLevel("foo")
	})
}
