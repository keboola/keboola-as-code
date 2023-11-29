package staging

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/test/testvalidation"
	"testing"
	"time"
)

func TestSliceUploadTrigger_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := testvalidation.TestCases[SliceUploadTrigger]{
		{
			Name: "empty",
			ExpectedError: `
- "count" is a required field
- "size" is a required field
- "interval" is a required field
`,
			Value: SliceUploadTrigger{},
		},
		{
			Name: "over maximum",
			ExpectedError: `
- "count" must be 10,000,000 or less
- "size" must be 50MB or less
- "interval" must be 30m0s or less
`,
			Value: SliceUploadTrigger{
				Count:    10000000 + 1,
				Size:     datasize.MustParseString("50MB") + 1,
				Interval: 30*time.Minute + 1,
			},
		},
		{
			Name:  "default",
			Value: DefaultSliceUploadTrigger(),
		},
	}

	// Run test cases
	cases.Run(t)
}
