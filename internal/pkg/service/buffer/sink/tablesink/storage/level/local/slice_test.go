package local

import (
	"context"
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/local/writer/disksync"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestSlice_Validation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		Name          string
		ExpectedError string
		Value         Slice
	}{
		{
			Name: "ok",
			Value: Slice{
				Dir:                "my-dir",
				Filename:           "slice.csv.gzip",
				AllocatedDiskSpace: 10 * datasize.KB,
				Compression:        compression.DefaultConfig(),
				DiskSync:           disksync.DefaultConfig(),
			},
		},
		{
			Name: "ok: IsEmpty=true",
			Value: Slice{
				Dir:                "my-dir",
				Filename:           "slice.csv.gzip",
				IsEmpty:            true,
				AllocatedDiskSpace: 10 * datasize.KB,
				Compression:        compression.DefaultConfig(),
				DiskSync:           disksync.DefaultConfig(),
			},
		},
		{
			Name: "ok: disk space allocation disabled",
			Value: Slice{
				Dir:                "my-dir",
				Filename:           "slice.csv.gzip",
				AllocatedDiskSpace: 0,
				Compression:        compression.DefaultConfig(),
				DiskSync:           disksync.DefaultConfig(),
			},
		},
		{
			Name:          "empty dir",
			ExpectedError: `"dir" is a required field`,
			Value: Slice{
				Dir:                "",
				Filename:           "slice.csv.gzip",
				AllocatedDiskSpace: 0,
				Compression:        compression.DefaultConfig(),
				DiskSync:           disksync.DefaultConfig(),
			},
		},
		{
			Name:          "empty filename",
			ExpectedError: `"filename" is a required field`,
			Value: Slice{
				Dir:                "my-dir",
				Filename:           "",
				AllocatedDiskSpace: 0,
				Compression:        compression.DefaultConfig(),
				DiskSync:           disksync.DefaultConfig(),
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
