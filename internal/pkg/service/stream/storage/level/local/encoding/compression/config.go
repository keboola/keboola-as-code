package compression

import (
	"compress/gzip"

	"github.com/c2h5oh/datasize"
	"github.com/klauspost/compress/zstd"
)

const (
	DefaultGZIPLevel     = gzip.BestSpeed // 1-9
	DefaultGZIPBlockSize = 256 * datasize.KB

	DefaultZSTDLevel      = zstd.SpeedDefault // 1-4
	DefaultZSDTWindowSize = 4 * datasize.MB
)

// Config configures compression writer and reader.
type Config struct {
	Type Type        `json:"type" configKey:"type" validate:"required,oneof=none gzip"  configUsage:"Compression type."`
	GZIP *GZIPConfig `json:"gzip,omitempty" configKey:"gzip" validate:"required_if=Type gzip"`
	ZSTD *ZSTDConfig `json:"zstd,omitempty" configKey:"-" validate:"required_if=Type zstd"` // hidden from the config, not supported by the Keboola platform
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Type *Type            `json:"type,omitempty"`
	GZIP *GZIPConfigPatch `json:"gzip,omitempty"`
	ZSTD *ZSTDConfigPatch `json:"zstd,omitempty"`
}

// GZIPConfig configures GZIP compression.
type GZIPConfig struct {
	Level          int                `json:"level" configKey:"level" validate:"min=1,max=9"  configUsage:"GZIP compression level: 1-9."`
	Implementation GZIPImplementation `json:"implementation" configKey:"implementation" validate:"required,oneof=standard fast parallel" configUsage:"GZIP implementation: standard, fast, parallel."`
	BlockSize      datasize.ByteSize  `json:"blockSize" configKey:"blockSize" validate:"required,minBytes=16kB,maxBytes=100MB" configUsage:"GZIP parallel block size."`
	Concurrency    int                `json:"concurrency" configKey:"concurrency" configUsage:"GZIP parallel concurrency, 0 = auto."`
}

// GZIPConfigPatch is same as the GZIPConfig, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type GZIPConfigPatch struct {
	Level          *int                `json:"level,omitempty"`
	Implementation *GZIPImplementation `json:"implementation,omitempty"`
	BlockSize      *datasize.ByteSize  `json:"blockSize,omitempty"`
	Concurrency    *int                `json:"concurrency,omitempty"`
}

// ZSTDConfig configures ZSTD compression.
type ZSTDConfig struct {
	Level       zstd.EncoderLevel `json:"level" configKey:"level" validate:"min=1,max=4" configUsage:"ZSTD compression level: fastest, default, better, best."`
	WindowSize  datasize.ByteSize `json:"windowSize" configKey:"windowSize" validate:"required,minBytes=1kB,maxBytes=512MB" configUsage:"ZSTD window size."`
	Concurrency int               `json:"concurrency" configKey:"concurrency" configUsage:"ZSTD concurrency, 0 = auto"`
}

// ZSTDConfigPatch is same as the ZSTDConfig, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ZSTDConfigPatch struct {
	Level       *zstd.EncoderLevel `json:"level,omitempty"`
	WindowSize  *datasize.ByteSize `json:"windowSize,omitempty"`
	Concurrency *int               `json:"concurrency,omitempty"`
}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{
		Type: TypeGZIP,
		GZIP: NewGZIPConfig().GZIP,
		ZSTD: NewZSTDConfig().ZSTD,
	}
}

func NewNoneConfig() Config {
	return Config{
		Type: TypeNone,
	}
}

func NewGZIPConfig() Config {
	return Config{
		Type: TypeGZIP,
		GZIP: &GZIPConfig{
			Level:          DefaultGZIPLevel,
			Implementation: DefaultGZIPImpl,
			BlockSize:      DefaultGZIPBlockSize,
			Concurrency:    0,
		},
	}
}

func NewZSTDConfig() Config {
	return Config{
		Type: TypeZSTD,
		ZSTD: &ZSTDConfig{
			Level:       DefaultZSTDLevel,
			WindowSize:  DefaultZSDTWindowSize,
			Concurrency: 0,
		},
	}
}

// Simplify removes unnecessary parts of the configuration.
func (c Config) Simplify() Config {
	if c.Type != TypeGZIP {
		c.GZIP = nil
	}
	if c.Type != TypeZSTD {
		c.ZSTD = nil
	}
	return c
}

// HasWriterInputBuffer returns true if the compression writer includes an input buffer.
func (c Config) HasWriterInputBuffer() bool {
	return c.Type == TypeGZIP && c.GZIP.Implementation == GZIPImplParallel
}
