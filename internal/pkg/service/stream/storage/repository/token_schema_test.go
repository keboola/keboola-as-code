package repository

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test"
)

func TestTokenSchema(t *testing.T) {
	t.Parallel()
	s := newTokenSchema(serde.NewJSON(serde.NoValidation))

	sinkKey := test.NewSinkKey()

	cases := []struct{ actual, expected string }{
		{
			s.Prefix(),
			"storage/secret/token/",
		},
		{
			s.InObject(sinkKey.ProjectID).Prefix(),
			"storage/secret/token/123/",
		},
		{
			s.InProject(sinkKey.ProjectID).Prefix(),
			"storage/secret/token/123/",
		},
		{
			s.InBranch(sinkKey.BranchKey).Prefix(),
			"storage/secret/token/123/456/",
		},
		{
			s.InSource(sinkKey.SourceKey).Prefix(),
			"storage/secret/token/123/456/my-source/",
		},
		{
			s.ByKey(sinkKey).Key(),
			"storage/secret/token/123/456/my-source/my-sink",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, fmt.Sprintf(`case "%d"`, i+1))
	}

	// Test panics
	assert.Panics(t, func() {
		// The last parent object type is Source
		s.InObject(key.SinkKey{})
	})
}
