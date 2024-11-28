package schema

import (
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
			"storage/keboola/job/",
		},
		{
			s.Prefix(),
			"storage/keboola/job/",
		},
		{
			s.In(jobKey.ProjectID).Prefix(),
			"storage/keboola/job/123/",
		},
		{
			s.In(jobKey.BranchKey).Prefix(),
			"storage/keboola/job/123/456/",
		},
		{
			s.In(jobKey.SourceKey).Prefix(),
			"storage/keboola/job/123/456/my-source/",
		},
		{
			s.InProject(jobKey.ProjectID).Prefix(),
			"storage/keboola/job/123/",
		},
		{
			s.InBranch(jobKey.BranchKey).Prefix(),
			"storage/keboola/job/123/456/",
		},
		{
			s.InSource(jobKey.SourceKey).Prefix(),
			"storage/keboola/job/123/456/my-source/",
		},
		{
			s.InSink(jobKey.SinkKey).Prefix(),
			"storage/keboola/job/123/456/my-source/my-sink/",
		},
		{
			s.ByKey(jobKey).Key(),
			"storage/keboola/job/123/456/my-source/my-sink/321",
		},
	}

	for i, c := range cases {
		assert.Equal(t, c.expected, c.actual, `case "%d"`, i+1)
	}
}

func TestSinkSchemaInState_In(t *testing.T) {
	t.Parallel()
	s := New(serde.NewJSON(serde.NoValidation))
	assert.Panics(t, func() {
		s.In("unexpected type")
	})
}
