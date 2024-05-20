package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func TestSinkSchema(t *testing.T) {
	t.Parallel()
	s := New(serde.NewJSON(serde.NoValidation))

	sourceKey := key.SourceKey{
		BranchKey: key.BranchKey{
			ProjectID: 123,
			BranchID:  456,
		},
		SourceID: "my-source",
	}

	sinkKey := key.SinkKey{
		SourceKey: sourceKey,
		SinkID:    "my-sink",
	}

	cases := []struct{ actual, expected string }{
		{
			s.Prefix(),
			"definition/sink/",
		},
		{
			s.Active().Prefix(),
			"definition/sink/active/",
		},
		{
			s.Active().In(sinkKey.ProjectID).Prefix(),
			"definition/sink/active/123/",
		},
		{
			s.Active().In(sinkKey.BranchKey).Prefix(),
			"definition/sink/active/123/456/",
		},
		{
			s.Active().In(sinkKey.SourceKey).Prefix(),
			"definition/sink/active/123/456/my-source/",
		},
		{
			s.Active().InProject(sinkKey.ProjectID).Prefix(),
			"definition/sink/active/123/",
		},
		{
			s.Active().InBranch(sinkKey.BranchKey).Prefix(),
			"definition/sink/active/123/456/",
		},
		{
			s.Active().InSource(sinkKey.SourceKey).Prefix(),
			"definition/sink/active/123/456/my-source/",
		},
		{
			s.Active().ByKey(sinkKey).Key(),
			"definition/sink/active/123/456/my-source/my-sink",
		},
		{
			s.Deleted().Prefix(),
			"definition/sink/deleted/",
		},
		{
			s.Deleted().InProject(sinkKey.ProjectID).Prefix(),
			"definition/sink/deleted/123/",
		},
		{
			s.Deleted().InBranch(sinkKey.BranchKey).Prefix(),
			"definition/sink/deleted/123/456/",
		},
		{
			s.Deleted().InSource(sinkKey.SourceKey).Prefix(),
			"definition/sink/deleted/123/456/my-source/",
		},
		{
			s.Deleted().ByKey(sinkKey).Key(),
			"definition/sink/deleted/123/456/my-source/my-sink",
		},
		{
			s.Versions().Prefix(),
			"definition/sink/version/",
		},
		{
			s.Versions().Of(sinkKey).Prefix(),
			"definition/sink/version/123/456/my-source/my-sink/",
		},
		{
			s.Versions().Of(sinkKey).Version(789).Key(),
			"definition/sink/version/123/456/my-source/my-sink/0000000789",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}
}

func TestSinkSchemaInState_In(t *testing.T) {
	t.Parallel()
	s := New(serde.NewJSON(serde.NoValidation))
	assert.Panics(t, func() {
		s.Active().In("unexpected type")
	})
}
