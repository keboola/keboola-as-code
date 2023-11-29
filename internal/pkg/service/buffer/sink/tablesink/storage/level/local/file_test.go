package local

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
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
				Dir:         "my-dir",
				Compression: compression.DefaultConfig(),
				DiskSync:    disksync.DefaultConfig(),
				VolumesAssignment: VolumesAssignment{
					PerPod: 1,
				},
			},
		},
		{
			Name:          "empty dir",
			ExpectedError: `"dir" is a required field`,
			Value: File{
				Dir:         "",
				Compression: compression.DefaultConfig(),
				DiskSync:    disksync.DefaultConfig(),
				VolumesAssignment: VolumesAssignment{
					PerPod: 1,
				},
			},
		},
		{
			Name:          "zero volumes per pod",
			ExpectedError: `"volumesAssignment.perPod" must be 1 or greater`,
			Value: File{
				Dir:         "my-dir",
				Compression: compression.DefaultConfig(),
				DiskSync:    disksync.DefaultConfig(),
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
		} else {
			if assert.Error(t, err, tc.Name) {
				assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
			}
		}
	}
}
