package run

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configmap"
	"github.com/keboola/keboola-as-code/pkg/lib/operation/project/remote/job/run"
)

func TestParseJobRunOptions(t *testing.T) {
	t.Parallel()

	f := Flags{
		Async:   configmap.NewValue(true),
		Timeout: configmap.NewValue("5m"),
	}

	parsed, err := parseJobRunOptions([]string{
		"1234/component1/config1",
		"1234/component1/config1",
		"4321/component2/config2",
		"component3/config3",
		"component3/config3@tag",
		"1234/component3/config3@tag",
	}, f)
	require.NoError(t, err)
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
