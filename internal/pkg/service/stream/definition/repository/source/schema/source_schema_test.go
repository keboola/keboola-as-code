package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func TestSourceSchema(t *testing.T) {
	t.Parallel()
	s := New(serde.NewJSON(serde.NoValidation))

	sourceKey := key.SourceKey{
		BranchKey: key.BranchKey{
			ProjectID: 123,
			BranchID:  456,
		},
		SourceID: "my-source",
	}

	cases := []struct{ actual, expected string }{
		{
			s.Prefix(),
			"definition/source/",
		},
		{
			s.Active().Prefix(),
			"definition/source/active/",
		},
		{
			s.Active().In(sourceKey.ProjectID).Prefix(),
			"definition/source/active/123/",
		},
		{
			s.Active().In(sourceKey.BranchKey).Prefix(),
			"definition/source/active/123/456/",
		},
		{
			s.Active().InProject(sourceKey.ProjectID).Prefix(),
			"definition/source/active/123/",
		},
		{
			s.Active().InBranch(sourceKey.BranchKey).Prefix(),
			"definition/source/active/123/456/",
		},
		{
			s.Active().ByKey(sourceKey).Key(),
			"definition/source/active/123/456/my-source",
		},
		{
			s.Deleted().Prefix(),
			"definition/source/deleted/",
		},
		{
			s.Deleted().InProject(sourceKey.ProjectID).Prefix(),
			"definition/source/deleted/123/",
		},
		{
			s.Deleted().InBranch(sourceKey.BranchKey).Prefix(),
			"definition/source/deleted/123/456/",
		},
		{
			s.Deleted().ByKey(sourceKey).Key(),
			"definition/source/deleted/123/456/my-source",
		},
		{
			s.Versions().Prefix(),
			"definition/source/version/",
		},
		{
			s.Versions().Of(sourceKey).Prefix(),
			"definition/source/version/123/456/my-source/",
		},
		{
			s.Versions().Of(sourceKey).Version(789).Key(),
			"definition/source/version/123/456/my-source/0000000789",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}
}

func TestSourceaInState_In(t *testing.T) {
	t.Parallel()
	s := New(serde.NewJSON(serde.NoValidation))
	assert.Panics(t, func() {
		s.Active().In("unexpected type")
	})
}
