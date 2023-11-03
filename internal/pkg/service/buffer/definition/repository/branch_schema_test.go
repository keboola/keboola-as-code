package repository

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
)

func TestBranchSchema(t *testing.T) {
	t.Parallel()
	s := newBranchSchema(serde.NewJSON(serde.NoValidation))

	branchKey := key.BranchKey{
		ProjectID: 123,
		BranchID:  456,
	}

	cases := []struct{ actual, expected string }{
		{
			s.Prefix(),
			"definition/branch/",
		},
		{
			s.Active().Prefix(),
			"definition/branch/active/",
		},
		{
			s.Active().ByKey(branchKey).Key(),
			"definition/branch/active/123/456",
		},
		{
			s.Deleted().Prefix(),
			"definition/branch/deleted/",
		},
		{
			s.Deleted().InProject(branchKey.ProjectID).Prefix(),
			"definition/branch/deleted/123/",
		},
		{
			s.Deleted().ByKey(branchKey).Key(),
			"definition/branch/deleted/123/456",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}
}
