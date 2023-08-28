package compression

import (
	"context"
	"github.com/keboola/keboola-as-code/internal/pkg/validator"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		Name          string
		ExpectedError string
		Config        Config
	}{
		{
			Name:          "empty",
			ExpectedError: `"type" is a required field`,
			Config:        Config{},
		},
		{
			Name:          "invalid type",
			ExpectedError: `"type" must be one of [none gzip zstd]`,
			Config: Config{
				Type: "foo",
			},
		},
		{
			Name:          "gzip: empty",
			ExpectedError: "- \"gzip.level\" must be 1 or greater\n- \"gzip.impl\" is a required field\n- \"gzip.blockSize\" is a required field",
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{},
			},
		},
		{
			Name:   "default  ok",
			Config: DefaultConfig(),
		},
		{
			Name: "none: ok",
			Config: Config{
				Type: TypeNone,
			},
		},
		{
			Name:   "none: default ok",
			Config: DefaultNoneConfig(),
		},
		{
			Name:          "none: unexpected gzip config",
			ExpectedError: `"gzip" should not be set`,
			Config: Config{
				Type: TypeNone,
				GZIP: &GZIPConfig{},
			},
		},
		{
			Name:          "none: unexpected zstd config",
			ExpectedError: `"zstd" should not be set`,
			Config: Config{
				Type: TypeNone,
				ZSTD: &ZSTDConfig{},
			},
		},
		{
			Name: "gzip: ok",
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:       DefaultGZIPLevel,
					Impl:        DefaultGZIPImpl,
					BlockSize:   DefaultGZIPBlockSize,
					Concurrency: 4,
				},
			},
		},
		{
			Name:   "gzip: default ok",
			Config: DefaultGZIPConfig(),
		},
		{
			Name:          "gzip: level under min",
			ExpectedError: `"gzip.level" must be 1 or greater`,
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:     0,
					Impl:      DefaultGZIPImpl,
					BlockSize: DefaultGZIPBlockSize,
				},
			},
		},
		{
			Name:          "gzip: level over max",
			ExpectedError: `"gzip.level" must be 9 or less`,
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:     10,
					Impl:      DefaultGZIPImpl,
					BlockSize: DefaultGZIPBlockSize,
				},
			},
		},
		{
			Name:          "gzip: unexpected impl",
			ExpectedError: `"gzip.impl" must be one of [standard fast parallel]`,
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:     DefaultGZIPLevel,
					Impl:      "foo",
					BlockSize: DefaultGZIPBlockSize,
				},
			},
		},
		{
			Name:          "gzip: block size under min",
			ExpectedError: `"gzip.blockSize" must be 16,384 or greater`,
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:     DefaultGZIPLevel,
					Impl:      DefaultGZIPImpl,
					BlockSize: 1,
				},
			},
		},
		{
			Name:          "gzip: block size over max",
			ExpectedError: `"gzip.blockSize" must be 104,857,600 or less`,
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:     DefaultGZIPLevel,
					Impl:      DefaultGZIPImpl,
					BlockSize: 1000000000,
				},
			},
		},
		{
			Name:          "gzip: unexpected zstd config",
			ExpectedError: `"zstd" should not be set`,
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:     DefaultGZIPLevel,
					Impl:      DefaultGZIPImpl,
					BlockSize: DefaultGZIPBlockSize,
				},
				ZSTD: &ZSTDConfig{},
			},
		},
		{
			Name: "zstd: ok",
			Config: Config{
				Type: TypeZSTD,
				ZSTD: &ZSTDConfig{
					Level:       DefaultGZIPLevel,
					WindowSize:  DefaultZSDTWindowSize,
					Concurrency: 4,
				},
			},
		},
		{
			Name:   "zstd: default ok",
			Config: DefaultZSTDConfig(),
		},
		{
			Name:          "zstd: level under min",
			ExpectedError: `"zstd.level" must be 1 or greater`,
			Config: Config{
				Type: TypeZSTD,
				ZSTD: &ZSTDConfig{
					Level:       0,
					WindowSize:  DefaultZSDTWindowSize,
					Concurrency: 4,
				},
			},
		},
		{
			Name:          "zstd: level over max",
			ExpectedError: `"zstd.level" must be 22 or less`,
			Config: Config{
				Type: TypeZSTD,
				ZSTD: &ZSTDConfig{
					Level:       100,
					WindowSize:  DefaultZSDTWindowSize,
					Concurrency: 4,
				},
			},
		},
		{
			Name:          "zstd: window size under min",
			ExpectedError: `"zstd.windowSize" must be 1,024 or greater`,
			Config: Config{
				Type: TypeZSTD,
				ZSTD: &ZSTDConfig{
					Level:       DefaultZSTDLevel,
					WindowSize:  1,
					Concurrency: 4,
				},
			},
		},
		{
			Name:          "zstd: window size over max",
			ExpectedError: `"zstd.windowSize" must be 536,870,912 or less`,
			Config: Config{
				Type: TypeZSTD,
				ZSTD: &ZSTDConfig{
					Level:       DefaultZSTDLevel,
					WindowSize:  1000000000,
					Concurrency: 4,
				},
			},
		},
		{
			Name:          "zstd: unexpected gzip config",
			ExpectedError: `"gzip" should not be set`,
			Config: Config{
				Type: TypeZSTD,
				GZIP: &GZIPConfig{},
				ZSTD: &ZSTDConfig{
					Level:       DefaultGZIPLevel,
					WindowSize:  DefaultZSDTWindowSize,
					Concurrency: 4,
				},
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
