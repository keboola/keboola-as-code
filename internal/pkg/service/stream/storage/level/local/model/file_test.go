package model

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/writer/writernode/diskalloc"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/volume/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestFile_Validation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		Name          string
		ExpectedError string
		Value         File
	}{
		{
			Name: "ok",
			Value: File{
				Dir:            "my-dir",
				Compression:    compression.NewConfig(),
				DiskSync:       disksync.NewConfig(),
				DiskAllocation: diskalloc.NewConfig(),
			},
		},
		{
			Name:          "empty dir",
			ExpectedError: `"dir" is a required field`,
			Value: File{
				Dir:            "",
				Compression:    compression.NewConfig(),
				DiskSync:       disksync.NewConfig(),
				DiskAllocation: diskalloc.NewConfig(),
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
