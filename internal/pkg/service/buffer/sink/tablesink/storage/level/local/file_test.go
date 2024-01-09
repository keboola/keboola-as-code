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
				Volumes: VolumesConfig{
					Count:                  1,
					RegistrationTTLSeconds: 10,
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
				Volumes: VolumesConfig{
					Count:                  1,
					RegistrationTTLSeconds: 10,
				},
			},
		},
		{
			Name: "empty volumes config",
			ExpectedError: `
- "volumes.count" is a required field
- "volumes.registrationTTL" is a required field
`,
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
		} else if assert.Error(t, err, tc.Name) {
			assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
		}
	}
}
