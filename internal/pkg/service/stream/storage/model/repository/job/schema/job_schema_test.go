package schema

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/etcdop/serde"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/definition/key"
)

func TestJobSchema(t *testing.T) {
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

	jobKey := key.JobKey{
		SinkKey: sinkKey,
		JobID:   "321",
	}

	cases := []struct{ actual, expected string }{
		{
			s.Prefix(),
			"storage/job/",
		},
		{
			s.Active().Prefix(),
			"storage/job/active/",
		},
		{
			s.Active().In(jobKey.ProjectID).Prefix(),
			"storage/job/active/123/",
		},
		{
			s.Active().In(jobKey.BranchKey).Prefix(),
			"storage/job/active/123/456/",
		},
		{
			s.Active().In(jobKey.SourceKey).Prefix(),
			"storage/job/active/123/456/my-source/",
		},
		{
			s.Active().InProject(jobKey.ProjectID).Prefix(),
			"storage/job/active/123/",
		},
		{
			s.Active().InBranch(jobKey.BranchKey).Prefix(),
			"storage/job/active/123/456/",
		},
		{
			s.Active().InSource(jobKey.SourceKey).Prefix(),
			"storage/job/active/123/456/my-source/",
		},
		{
			s.Active().InSink(jobKey.SinkKey).Prefix(),
			"storage/job/active/123/456/my-source/my-sink/",
		},
		{
			s.Active().ByKey(jobKey).Key(),
			"storage/job/active/123/456/my-source/my-sink/321",
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
