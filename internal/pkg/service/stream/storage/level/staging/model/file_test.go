package model

import (
	"testing"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/utctime"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/compression"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestFile_Validation(t *testing.T) {
	t.Parallel()

	cases := testvalidation.TestCases[File]{
		{
			Name: "empty",
			ExpectedError: `
- "provider" is a required field
- "compression" is a required field
- "expiration" is a required field
`,
			Value: File{},
		},
		{
			Name: "ok",
			Value: File{
				Provider:    "foo",
				Compression: compression.NewConfig(),
				Expiration:  utctime.MustParse("2006-01-02T15:04:05.000Z"),
			},
		},
	}

	cases.Run(t)
}
