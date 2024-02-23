package target_test

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestConfig_Validation(t *testing.T) {
	t.Parallel()

	overMaximumCfg := target.NewConfig()
	overMaximumCfg.Import.Trigger = target.ImportTrigger{
		Count:    10000000 + 1,
		Size:     datasize.MustParseString("500MB") + 1,
		Interval: duration.From(24*time.Hour + 1),
	}

	// Test cases
	cases := testvalidation.TestCases[target.Config]{
		{
			Name: "empty",
			ExpectedError: `
- "import.minInterval" is a required field
- "import.trigger.count" is a required field
- "import.trigger.size" is a required field
- "import.trigger.interval" is a required field
`,
			Value: target.Config{},
		},
		{
			Name: "over maximum",
			ExpectedError: `
- "import.trigger.count" must be 10,000,000 or less
- "import.trigger.size" must be 500MB or less
- "import.trigger.interval" must be 24h0m0s or less
`,
			Value: overMaximumCfg,
		},
		{
			Name:  "default",
			Value: target.NewConfig(),
		},
	}

	// Run test cases
	cases.Run(t)
}
