package compression

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/keboola/keboola-as-code/internal/pkg/validator"
)

func TestConfig_Validation(t *testing.T) {
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
			ExpectedError: `"type" must be one of [none gzip]`,
			Config: Config{
				Type: "foo",
			},
		},
		{
			Name:          "gzip: empty",
			ExpectedError: "- \"gzip.level\" must be 1 or greater\n- \"gzip.implementation\" is a required field\n- \"gzip.blockSize\" is a required field",
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{},
			},
		},
		{
			Name:   "default  ok",
			Config: NewConfig(),
		},
		{
			Name: "none: ok",
			Config: Config{
				Type: TypeNone,
			},
		},
		{
			Name:   "none: default ok",
			Config: NewNoneConfig(),
		},
		{
			Name: "gzip: ok",
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:          DefaultGZIPLevel,
					Implementation: DefaultGZIPImpl,
					BlockSize:      DefaultGZIPBlockSize,
					Concurrency:    4,
				},
			},
		},
		{
			Name:   "gzip: default ok",
			Config: NewGZIPConfig(),
		},
		{
			Name:          "gzip: level under min",
			ExpectedError: `"gzip.level" must be 1 or greater`,
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:          0,
					Implementation: DefaultGZIPImpl,
					BlockSize:      DefaultGZIPBlockSize,
				},
			},
		},
		{
			Name:          "gzip: level over max",
			ExpectedError: `"gzip.level" must be 9 or less`,
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:          10,
					Implementation: DefaultGZIPImpl,
					BlockSize:      DefaultGZIPBlockSize,
				},
			},
		},
		{
			Name:          "gzip: unexpected impl",
			ExpectedError: `"gzip.implementation" must be one of [standard fast parallel]`,
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:          DefaultGZIPLevel,
					Implementation: "foo",
					BlockSize:      DefaultGZIPBlockSize,
				},
			},
		},
		{
			Name:          "gzip: block size under min",
			ExpectedError: `"gzip.blockSize" must be 16KB or greater`,
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:          DefaultGZIPLevel,
					Implementation: DefaultGZIPImpl,
					BlockSize:      1,
				},
			},
		},
		{
			Name:          "gzip: block size over max",
			ExpectedError: `"gzip.blockSize" must be 100MB or less`,
			Config: Config{
				Type: TypeGZIP,
				GZIP: &GZIPConfig{
					Level:          DefaultGZIPLevel,
					Implementation: DefaultGZIPImpl,
					BlockSize:      1000000000,
				},
			},
		},
		/*{
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
			Config: NewZSTDConfig(),
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
			ExpectedError: `"zstd.level" must be 4 or less`,
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
			ExpectedError: `"zstd.windowSize" must be 1KB or greater`,
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
			ExpectedError: `"zstd.windowSize" must be 512MB or less`,
			Config: Config{
				Type: TypeZSTD,
				ZSTD: &ZSTDConfig{
					Level:       DefaultZSTDLevel,
					WindowSize:  1000000000,
					Concurrency: 4,
				},
			},
		},*/
	}

	// Run test cases
	ctx := t.Context()
	val := validator.New()
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			err := val.Validate(ctx, tc.Config)
			if tc.ExpectedError == "" {
				require.NoError(t, err, tc.Name)
			} else if assert.Error(t, err, tc.Name) {
				assert.Equal(t, strings.TrimSpace(tc.ExpectedError), strings.TrimSpace(err.Error()), tc.Name)
			}
		})
	}
}

func TestConfig_Simplify(t *testing.T) {
	t.Parallel()

	gzipCfg := NewGZIPConfig().GZIP
	zstdCfg := NewZSTDConfig().ZSTD

	// None
	cfg := Config{Type: TypeNone, GZIP: gzipCfg, ZSTD: zstdCfg}
	assert.Equal(t, Config{Type: TypeNone}, cfg.Simplify())

	// GZIP
	cfg = Config{Type: TypeGZIP, GZIP: gzipCfg, ZSTD: zstdCfg}
	assert.Equal(t, Config{Type: TypeGZIP, GZIP: gzipCfg}, cfg.Simplify())

	// ZSTD
	cfg = Config{Type: TypeZSTD, GZIP: gzipCfg, ZSTD: zstdCfg}
	assert.Equal(t, Config{Type: TypeZSTD, ZSTD: zstdCfg}, cfg.Simplify())
}
