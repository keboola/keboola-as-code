package target_test

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/target"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test/testvalidation"
)

func TestConfig_With(t *testing.T) {
	t.Parallel()

	defaultCfg := target.NewConfig()

	// Apply empty patch
	assert.Equal(t, defaultCfg, defaultCfg.With(target.ConfigPatch{}))

	// Apply full patch
	patchedCfg := defaultCfg.With(target.ConfigPatch{
		Import: &target.ImportConfigPatch{
			Trigger: &target.ImportTriggerPatch{
				Interval: test.Ptr(456 * time.Millisecond),
			},
		},
	})
	expectedCfg := defaultCfg
	expectedCfg.Import.Trigger.Interval = 456 * time.Millisecond
	assert.Equal(t, expectedCfg, patchedCfg)
}

func TestConfig_Validation(t *testing.T) {
	t.Parallel()

	overMaximumCfg := target.NewConfig()
	overMaximumCfg.Import.Trigger = target.ImportTrigger{
		Count:    10000000 + 1,
		Size:     datasize.MustParseString("500MB") + 1,
		Interval: 24*time.Hour + 1,
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
