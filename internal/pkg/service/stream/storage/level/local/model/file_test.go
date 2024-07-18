package model

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/diskwriter/diskalloc"
	encoding "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/assignment"
	volume "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/model"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestFile_Validation(t *testing.T) {
	t.Parallel()

	volumeAssignment := assignment.Assignment{
		Config: assignment.Config{
			Count:          2,
			PreferredTypes: []string{"foo", "bar"},
		},
		Volumes: []volume.ID{"my-volume-1", "my-volume-2"},
	}

	cases := []struct {
		Name          string
		ExpectedError string
		Value         File
	}{
		{
			Name: "ok",
			Value: File{
				Dir:        "my-dir",
				Assignment: volumeAssignment,
				Allocation: diskalloc.NewConfig(),
				Encoding:   encoding.NewConfig(),
			},
		},
		{
			Name:          "empty dir",
			ExpectedError: `"dir" is a required field`,
			Value: File{
				Dir:        "",
				Assignment: volumeAssignment,
				Allocation: diskalloc.NewConfig(),
				Encoding:   encoding.NewConfig(),
			},
		},
	}

	// Run test cases
	ctx := context.Background()
	val := validator.New()
	for _, tc := range cases {
		err := val.Validate(ctx, tc.Value)
		if tc.ExpectedError == "" {
			assert.NoError(t, err, tc.Name)
		} else if assert.Error(t, err, tc.Name) {
			assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
		}
	}
}
