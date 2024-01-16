package disksync

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/assert"

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
			Name:          "disabled mode: unexpected fields",
			ExpectedError: "- \"wait\" should not be set\n- \"checkInterval\" should not be set\n- \"countTrigger\" should not be set\n- \"bytesTrigger\" should not be set\n- \"intervalTrigger\" should not be set",
			Config: Config{
				Mode:            ModeDisabled,
				Wait:            true,
				CheckInterval:   1,
				CountTrigger:    1,
				BytesTrigger:    1,
				IntervalTrigger: 1,
			},
		},
		{
			Name:          "disk mode: empty",
			ExpectedError: "- \"checkInterval\" is a required field\n- \"countTrigger\" is a required field\n- \"bytesTrigger\" is a required field\n- \"intervalTrigger\" is a required field",
			Config: Config{
				Mode: ModeDisk,
			},
		},
		{
			Name: "disk mode: ok",
			Config: Config{
				Mode:            ModeDisk,
				Wait:            true,
				CheckInterval:   10 * time.Millisecond,
				CountTrigger:    100,
				BytesTrigger:    100,
				IntervalTrigger: 10 * time.Millisecond,
			},
		},
		{
			Name:          "cache mode: cache",
			ExpectedError: "- \"checkInterval\" is a required field\n- \"countTrigger\" is a required field\n- \"bytesTrigger\" is a required field\n- \"intervalTrigger\" is a required field",
			Config: Config{
				Mode: ModeCache,
			},
		},
		{
			Name: "cache mode: ok",
			Config: Config{
				Mode:            ModeCache,
				Wait:            true,
				CheckInterval:   10 * time.Millisecond,
				CountTrigger:    100,
				BytesTrigger:    100,
				IntervalTrigger: 10 * time.Millisecond,
			},
		},
		{
			Name:          "check interval: over max",
			ExpectedError: `"checkInterval" must be 2s or less`,
			Config: Config{
				Mode:            ModeDisk,
				Wait:            true,
				CheckInterval:   10 * time.Second,
				CountTrigger:    100,
				BytesTrigger:    100,
				IntervalTrigger: 10 * time.Millisecond,
			},
		},
		{
			Name:          "count trigger: over max",
			ExpectedError: `"countTrigger" must be 1,000,000 or less`,
			Config: Config{
				Mode:            ModeDisk,
				Wait:            true,
				CheckInterval:   10 * time.Millisecond,
				CountTrigger:    2000000,
				BytesTrigger:    100,
				IntervalTrigger: 10 * time.Millisecond,
			},
		},
		{
			Name:          "bytes trigger: over max",
			ExpectedError: `"bytesTrigger" must be 100MB or less`,
			Config: Config{
				Mode:            ModeDisk,
				Wait:            true,
				CheckInterval:   10 * time.Millisecond,
				CountTrigger:    100,
				BytesTrigger:    1 * datasize.GB,
				IntervalTrigger: 10 * time.Millisecond,
			},
		},
		{
			Name:          "interval trigger: negative",
			ExpectedError: `"intervalTrigger" must be 0 or greater`,
			Config: Config{
				Mode:            ModeDisk,
				Wait:            true,
				CheckInterval:   10 * time.Millisecond,
				CountTrigger:    100,
				BytesTrigger:    100,
				IntervalTrigger: -10 * time.Millisecond,
			},
		},
		{
			Name:          "interval trigger: over max",
			ExpectedError: `"intervalTrigger" must be 2s or less`,
			Config: Config{
				Mode:            ModeDisk,
				Wait:            true,
				CheckInterval:   10 * time.Millisecond,
				CountTrigger:    100,
				BytesTrigger:    100,
				IntervalTrigger: 10 * time.Second,
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
