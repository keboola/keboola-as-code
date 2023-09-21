package repository

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

type keyTestCase struct{ actual, expected string }

func TestSchema(t *testing.T) {
	t.Parallel()
	s := newSchema(serde.NewJSON(serde.NoValidation))
	sliceKey := test.NewSliceKey()

	cases := []keyTestCase{
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
			s.InLevel(storage.LevelLocal).InProject(sliceKey.ProjectID).Prefix(),
			"storage/stats/local/123/",
		},
		{
			s.InLevel(storage.LevelLocal).InReceiver(sliceKey.ReceiverKey).Prefix(),
			"storage/stats/local/123/my-receiver/",
		},
		{
			s.InLevel(storage.LevelLocal).InReceiver(sliceKey.ReceiverKey).Sum().Key(),
			"storage/stats/local/123/my-receiver/_sum",
		},
		{
			s.InLevel(storage.LevelLocal).InExport(sliceKey.ExportKey).Prefix(),
			"storage/stats/local/123/my-receiver/my-export/",
		},
		{
			s.InLevel(storage.LevelLocal).InExport(sliceKey.ExportKey).Sum().Key(),
			"storage/stats/local/123/my-receiver/my-export/_sum",
		},
		{
			s.InLevel(storage.LevelLocal).InFile(sliceKey.FileKey).Prefix(),
			"storage/stats/local/123/my-receiver/my-export/2000-01-01T19:00:00.000Z/",
		},
		{
			s.InLevel(storage.LevelLocal).InSlice(sliceKey).Key(),
			"storage/stats/local/123/my-receiver/my-export/2000-01-01T19:00:00.000Z/my-volume/2000-01-01T20:00:00.000Z/value",
		},
		{
			s.InLevel(storage.LevelLocal).InParentOf(sliceKey.ReceiverKey).Prefix(),
			"storage/stats/local/123/",
		},
		{
			s.InLevel(storage.LevelLocal).InParentOf(sliceKey.ExportKey).Prefix(),
			"storage/stats/local/123/my-receiver/",
		},
		{
			s.InLevel(storage.LevelLocal).InParentOf(sliceKey.FileKey).Prefix(),
			"storage/stats/local/123/my-receiver/my-export/",
		},
		{
			s.InLevel(storage.LevelLocal).InParentOf(sliceKey).Prefix(),
			"storage/stats/local/123/my-receiver/my-export/2000-01-01T19:00:00.000Z/",
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
