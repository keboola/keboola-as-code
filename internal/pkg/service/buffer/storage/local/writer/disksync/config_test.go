package disksync

import (
	"context"
	"github.com/c2h5oh/datasize"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
	"time"
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
			Config: DefaultConfig(),
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
			ExpectedError: "- \"wait\" should not be set\n- \"bytesTrigger\" should not be set\n- \"intervalTrigger\" should not be set",
			Config: Config{
				Mode:            ModeDisabled,
				Wait:            true,
				BytesTrigger:    1,
				IntervalTrigger: 1,
			},
		},
		{
			Name:          "disk mode: empty",
			ExpectedError: "- \"bytesTrigger\" is a required field\n- \"intervalTrigger\" is a required field",
			Config: Config{
				Mode: ModeDisk,
			},
		},
		{
			Name: "disk mode: ok",
			Config: Config{
				Mode:            ModeDisk,
				Wait:            true,
				BytesTrigger:    100,
				IntervalTrigger: 10 * time.Millisecond,
			},
		},
		{
			Name:          "cache mode: cache",
			ExpectedError: "- \"bytesTrigger\" is a required field\n- \"intervalTrigger\" is a required field",
			Config: Config{
				Mode: ModeCache,
			},
		},
		{
			Name: "cache mode: ok",
			Config: Config{
				Mode:            ModeCache,
				Wait:            true,
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
		} else {
			if assert.Error(t, err, tc.Name) {
				assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
			}
		}
	}
}
