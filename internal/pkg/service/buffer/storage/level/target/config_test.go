package target

import (
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/storage/test/testvalidation"
	"testing"
	"time"
)

func TestFileImportTrigger_Validation(t *testing.T) {
	t.Parallel()

	// Test cases
	cases := testvalidation.TestCases[FileImportTrigger]{
		{
			Name: "empty",
			ExpectedError: `
- "count" is a required field
- "size" is a required field
- "interval" is a required field
`,
			Value: FileImportTrigger{},
		},
		{
			Name: "over maximum",
			ExpectedError: `
- "count" must be 10,000,000 or less
- "size" must be 500MB or less
- "interval" must be 24h0m0s or less
`,
			Value: FileImportTrigger{
				Count:    10000000 + 1,
				Size:     datasize.MustParseString("500MB") + 1,
				Interval: 24*time.Hour + 1,
			},
		},
		{
			Name:  "default",
			Value: DefaultFileImportTrigger(),
		},
	}

	// Run test cases
	cases.Run(t)
}
