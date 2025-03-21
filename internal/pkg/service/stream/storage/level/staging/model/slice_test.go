package model

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/local/encoding/compression"
	staging "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

const (
	testStagingFileProvider = FileProvider("keboola")
)

func TestSlice_Validation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		Name          string
		ExpectedError string
		Value         Slice
	}{
		{
			Name:          "empty",
			ExpectedError: `"path" is a required field`,
			Value: Slice{
				Provider:    testStagingFileProvider,
				Compression: compression.NewConfig(),
				Upload:      staging.NewConfig().Upload,
			},
		},
		{
			Name: "ok",
			Value: Slice{
				Path:        "my-slice.csv.gzip",
				Provider:    testStagingFileProvider,
				Compression: compression.NewConfig(),
				Upload:      staging.NewConfig().Upload,
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
