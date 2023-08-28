package staging

import (
	"context"
	"github.com/keboola/go-client/pkg/keboola"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestFile_Validation(t *testing.T) {
	cases := []struct {
		Name          string
		ExpectedError string
		Value         File
	}{
		{
			Name:          "empty",
			ExpectedError: "- \"credentials\" is a required field\n- \"credentialsExpiration\" is a required field",
			Value: File{
				Compression: compression.DefaultConfig(),
			},
		},
		{
			Name: "ok",
			Value: File{
				Compression:                 compression.DefaultConfig(),
				UploadCredentials:           &keboola.FileUploadCredentials{},
				UploadCredentialsExpiration: utctime.MustParse("2006-01-02T15:04:05.000Z"),
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
