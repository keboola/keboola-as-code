package model

import (
	"testing"

	target "github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestFile_Validation(t *testing.T) {
	cases := testvalidation.TestCases[Target]{
		{
			Name: "empty",
			ExpectedError: `
- "provider" is a required field
- "import.minInterval" is a required field
- "import.trigger.count" is a required field
- "import.trigger.size" is a required field
- "import.trigger.interval" is a required field
`,
			Value: Target{},
		},
		{
			Name: "ok",
			Value: Target{
				Provider: "foo",
				Import:   target.NewConfig().Import,
			},
		},
	}

	cases.Run(t)
}
