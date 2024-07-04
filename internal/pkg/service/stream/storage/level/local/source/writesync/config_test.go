package writesync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

	"github.com/keboola/keboola-as-code/internal/pkg/service/common/duration"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		Name          string
		ExpectedError string
		Config        Config
	}{
		{
			Name:   "default: ok",
			Config: NewConfig(),
		},
		{
			Name:          "invalid mode",
			ExpectedError: `"mode" must be one of [disabled disk cache]`,
			Config: Config{
				Mode: "invalid",
			},
		},
		{
			Name: "disabled: ok",
			Config: Config{
				Mode: ModeDisabled,
			},
		},
		{
			Name: "disk mode: empty",
			ExpectedError: `
- "checkInterval" is a required field
- "countTrigger" is a required field
- "uncompressedBytesTrigger" is a required field
- "compressedBytesTrigger" is a required field
- "intervalTrigger" is a required field`,
			Config: Config{
				Mode: ModeDisk,
			},
		},
		{
			Name: "disk mode: ok",
			Config: Config{
				Mode:                     ModeDisk,
				Wait:                     true,
				CheckInterval:            duration.From(10 * time.Millisecond),
				CountTrigger:             100,
				UncompressedBytesTrigger: 100,
				CompressedBytesTrigger:   100,
				IntervalTrigger:          duration.From(10 * time.Millisecond),
			},
		},
		{
			Name: "cache mode: cache",
			ExpectedError: `
- "checkInterval" is a required field
- "countTrigger" is a required field
- "uncompressedBytesTrigger" is a required field
- "compressedBytesTrigger" is a required field
- "intervalTrigger" is a required field`,
			Config: Config{
				Mode: ModeCache,
			},
		},
		{
			Name: "cache mode: ok",
			Config: Config{
				Mode:                     ModeCache,
				Wait:                     true,
				CheckInterval:            duration.From(10 * time.Millisecond),
				CountTrigger:             100,
				UncompressedBytesTrigger: 100,
				CompressedBytesTrigger:   100,
				IntervalTrigger:          duration.From(10 * time.Millisecond),
			},
		},
		{
			Name:          "check interval: over max",
			ExpectedError: `"checkInterval" must be 2s or less`,
			Config: Config{
				Mode:                     ModeDisk,
				Wait:                     true,
				CheckInterval:            duration.From(10 * time.Second),
				CountTrigger:             100,
				UncompressedBytesTrigger: 100,
				CompressedBytesTrigger:   100,
				IntervalTrigger:          duration.From(10 * time.Millisecond),
			},
		},
		{
			Name:          "count trigger: over max",
			ExpectedError: `"countTrigger" must be 1,000,000 or less`,
			Config: Config{
				Mode:                     ModeDisk,
				Wait:                     true,
				CheckInterval:            duration.From(10 * time.Millisecond),
				CountTrigger:             2000000,
				UncompressedBytesTrigger: 100,
				CompressedBytesTrigger:   100,
				IntervalTrigger:          duration.From(10 * time.Millisecond),
			},
		},
		{
			Name:          "uncompressed trigger: over max",
			ExpectedError: `"uncompressedBytesTrigger" must be 500MB or less`,
			Config: Config{
				Mode:                     ModeDisk,
				Wait:                     true,
				CheckInterval:            duration.From(10 * time.Millisecond),
				CountTrigger:             100,
				UncompressedBytesTrigger: 1 * datasize.GB,
				CompressedBytesTrigger:   100,
				IntervalTrigger:          duration.From(10 * time.Millisecond),
			},
		},
		{
			Name:          "compressed bytes trigger: over max",
			ExpectedError: `"compressedBytesTrigger" must be 100MB or less`,
			Config: Config{
				Mode:                     ModeDisk,
				Wait:                     true,
				CheckInterval:            duration.From(10 * time.Millisecond),
				CountTrigger:             100,
				UncompressedBytesTrigger: 100,
				CompressedBytesTrigger:   1 * datasize.GB,
				IntervalTrigger:          duration.From(10 * time.Millisecond),
			},
		},
		{
			Name:          "interval trigger: negative",
			ExpectedError: `"intervalTrigger" must be 0 or greater`,
			Config: Config{
				Mode:                     ModeDisk,
				Wait:                     true,
				CheckInterval:            duration.From(10 * time.Millisecond),
				CountTrigger:             123,
				UncompressedBytesTrigger: 100,
				CompressedBytesTrigger:   100,
				IntervalTrigger:          duration.From(-10 * time.Millisecond),
			},
		},
		{
			Name:          "interval trigger: over max",
			ExpectedError: `"intervalTrigger" must be 2s or less`,
			Config: Config{
				Mode:                     ModeDisk,
				Wait:                     true,
				CheckInterval:            duration.From(10 * time.Millisecond),
				CountTrigger:             123,
				UncompressedBytesTrigger: 100,
				CompressedBytesTrigger:   100,
				IntervalTrigger:          duration.From(10 * time.Second),
			},
		},
	}

	// Run test cases
	ctx := context.Background()
	val := validator.New()
	for _, tc := range cases {
		err := val.Validate(ctx, tc.Config)
		if tc.ExpectedError == "" {
			assert.NoError(t, err, tc.Name)
		} else if assert.Error(t, err, tc.Name) {
			assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
		}
	}
}
