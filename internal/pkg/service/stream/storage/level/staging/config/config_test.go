package config_test

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/level/staging/config"
	"github.com/keboola/keboola-as-code/internal/pkg/service/stream/storage/test/testvalidation"
)

func TestConfig_Validation(t *testing.T) {
	t.Parallel()

	overMaximumCfg := config.NewConfig()
	overMaximumCfg.Upload.Trigger = config.UploadTrigger{
		Count:    10000000 + 1,
		Size:     datasize.MustParseString("50MB") + 1,
		Interval: duration.From(30*time.Minute + 1),
	}

	// Test cases
	cases := testvalidation.TestCases[config.Config]{
		{
			Name: "empty",
			ExpectedError: `
- "operator.checkInterval" is a required field
- "operator.sliceRotationTimeout" is a required field
- "operator.sliceCloseTimeout" is a required field
- "operator.sliceUploadTimeout" is a required field
- "upload.trigger.count" is a required field
- "upload.trigger.size" is a required field
- "upload.trigger.interval" is a required field
`,
			Value: config.Config{},
		},
		{
			Name: "over maximum",
			ExpectedError: `
- "upload.trigger.count" must be 10,000,000 or less
- "upload.trigger.size" must be 50MB or less
- "upload.trigger.interval" must be 30m0s or less
`,
			Value: overMaximumCfg,
		},
		{
			Name:  "default",
			Value: config.NewConfig(),
		},
	}

	// Run test cases
	cases.Run(t)
}
