package job

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/cli/options"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/job/run"
)

func TestParseJobRunOptions(t *testing.T) {
	t.Parallel()

	opts := options.New()
	opts.Set("timeout", "5m")
	opts.Set("async", true)

	parsed, err := parseJobRunOptions(opts, []string{
		"1234/component1/config1",
		"1234/component1/config1",
		"4321/component2/config2",
		"component3/config3",
		"component3/config3@tag",
		"1234/component3/config3@tag",
	})
	assert.NoError(t, err)
	assert.Equal(t,
		run.RunOptions{
			Jobs: []*run.Job{
				{
					BranchID:    1234,
					ComponentID: "component1",
					ConfigID:    "config1",
				},
				{
					BranchID:    1234,
					ComponentID: "component1",
					ConfigID:    "config1",
				},
				{
					BranchID:    4321,
					ComponentID: "component2",
					ConfigID:    "config2",
				},
				{
					ComponentID: "component3",
					ConfigID:    "config3",
				},
				{
					ComponentID: "component3",
					ConfigID:    "config3",
					Tag:         "tag",
				},
				{
					BranchID:    1234,
					ComponentID: "component3",
					ConfigID:    "config3",
					Tag:         "tag",
				},
			},
			Async:   true,
			Timeout: time.Minute * 5,
		},
		parsed,
	)
}
