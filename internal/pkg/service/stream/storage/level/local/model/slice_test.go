package model

import (
	"strings"
	"testing"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

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
				FilenamePrefix:     "slice",
				FilenameExtension:  "csv.gzip",
				AllocatedDiskSpace: 10 * datasize.KB,
			},
		},
		{
			Name: "ok: IsEmpty=true",
			Value: Slice{
				Dir:                "my-dir",
				FilenamePrefix:     "slice",
				FilenameExtension:  "csv.gzip",
				IsEmpty:            true,
				AllocatedDiskSpace: 10 * datasize.KB,
			},
		},
		{
			Name: "ok: disk space allocation disabled",
			Value: Slice{
				Dir:                "my-dir",
				FilenamePrefix:     "slice",
				FilenameExtension:  "csv.gzip",
				AllocatedDiskSpace: 0,
			},
		},
		{
			Name:          "empty dir",
			ExpectedError: `"dir" is a required field`,
			Value: Slice{
				Dir:                "",
				FilenamePrefix:     "slice",
				FilenameExtension:  "csv.gzip",
				AllocatedDiskSpace: 0,
			},
		},
		{
			Name:          "empty filenamePrefix",
			ExpectedError: `"filenamePrefix" is a required field`,
			Value: Slice{
				Dir:                "my-dir",
				FilenamePrefix:     "",
				FilenameExtension:  "csv",
				AllocatedDiskSpace: 0,
			},
		},
		{
			Name:          "empty filenameExtension",
			ExpectedError: `"filenameExtension" is a required field`,
			Value: Slice{
				Dir:                "my-dir",
				FilenamePrefix:     "slice",
				FilenameExtension:  "",
				AllocatedDiskSpace: 0,
			},
		},
	}

	// Run test cases
	ctx := t.Context()
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
