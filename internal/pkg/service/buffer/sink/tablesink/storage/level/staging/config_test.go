package staging_test

import (
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/level/staging"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test"
	"github.com/keboola/keboola-as-code/internal/pkg/service/buffer/sink/tablesink/storage/test/testvalidation"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/configpatch"
	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
)

func TestConfig_With(t *testing.T) {
	t.Parallel()

	defaultConfig := staging.NewConfig()

	// Apply empty patch
	patchedConfig1 := defaultConfig
	require.NoError(t, configpatch.Apply(patchedConfig1, staging.ConfigPatch{}))
	assert.Equal(t, defaultConfig, patchedConfig1)

	// Apply full patch
	patchedConfig2 := patchedConfig1
	require.NoError(t, configpatch.Apply(patchedConfig2, staging.ConfigPatch{
		MaxSlicesPerFile: test.Ptr(123),
		Upload: &staging.UploadConfigPatch{
			Trigger: &staging.UploadTriggerPatch{
				Interval: test.Ptr(duration.From(456 * time.Millisecond)),
			},
		},
	}))
	expectedCfg := defaultConfig
	expectedCfg.MaxSlicesPerFile = 123
	expectedCfg.Upload.Trigger.Interval = duration.From(456 * time.Millisecond)
	assert.Equal(t, expectedCfg, patchedConfig2)
}

func TestConfig_Validation(t *testing.T) {
	t.Parallel()

	overMaximumCfg := staging.NewConfig()
	overMaximumCfg.Upload.Trigger = staging.UploadTrigger{
		Count:    10000000 + 1,
		Size:     datasize.MustParseString("50MB") + 1,
		Interval: duration.From(30*time.Minute + 1),
	}

	// Test cases
	cases := testvalidation.TestCases[staging.Config]{
		{
			Name: "empty",
			ExpectedError: `
- "maxSlicesPerFile" is a required field
- "parallelFileCreateLimit" is a required field
- "upload.minInterval" is a required field
- "upload.trigger.count" is a required field
- "upload.trigger.size" is a required field
- "upload.trigger.interval" is a required field
`,
			Value: staging.Config{},
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
			Value: staging.NewConfig(),
		},
	}

	// Run test cases
	cases.Run(t)
}
