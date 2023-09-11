package target

import (
	"context"
	"strings"
	"testing"

	"github.com/keboola/go-client/pkg/keboola"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestFile_Validation(t *testing.T) {
	cases := []struct {
		Name          string
		ExpectedError string
		Value         File
	}{
		{
			Name:          "empty",
			ExpectedError: `"tableId" is a required field`,
			Value:         File{},
		},
		{
			Name: "ok",
			Value: File{
				TableID:    keboola.MustParseTableID("in.bucket.table"),
				StorageJob: &keboola.StorageJob{},
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
