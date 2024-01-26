package compression

import (
	"compress/gzip"

	"github.com/c2h5oh/datasize"
	"github.com/klauspost/compress/zstd"
)

const (
	DefaultGZIPLevel     = gzip.BestSpeed // 1-9
	DefaultGZIPBlockSize = 256 * datasize.KB

	DefaultZSTDLevel      = zstd.SpeedFastest // 1-4
	DefaultZSDTWindowSize = 1 * datasize.MB
)

// Config configures compression writer and reader.
type Config struct {
	Type Type        `json:"type" configKey:"type" validate:"required,oneof=none gzip zstd"  configUsage:"Compression type."`
	GZIP *GZIPConfig `json:"gzip,omitempty" configKey:"gzip" validate:"required_if=Type gzip"`
	ZSTD *ZSTDConfig `json:"zstd,omitempty" configKey:"zstd" validate:"required_if=Type zstd"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It is part of the definition.TableSink structure to allow modification of the default configuration.
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
// It is part of the definition.TableSink structure to allow modification of the default configuration.
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
// It is part of the definition.TableSink structure to allow modification of the default configuration.
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

// With copies values from the ConfigPatch, if any.
func (c Config) With(v ConfigPatch) Config {
	if v.Type != nil {
		c.Type = *v.Type
	}
	if c.GZIP != nil && v.GZIP != nil {
		patched := c.GZIP.With(*v.GZIP)
		c.GZIP = &patched
	}
	if c.ZSTD != nil && v.ZSTD != nil {
		patched := c.ZSTD.With(*v.ZSTD)
		c.ZSTD = &patched
	}
	return c
}

// With copies values from the GZIPConfigPatch, if any.
func (c GZIPConfig) With(v GZIPConfigPatch) GZIPConfig {
	if v.Level != nil {
		c.Level = *v.Level
	}
	if v.Implementation != nil {
		c.Implementation = *v.Implementation
	}
	if v.BlockSize != nil {
		c.BlockSize = *v.BlockSize
	}
	if v.Concurrency != nil {
		c.Concurrency = *v.Concurrency
	}
	return c
}

// With copies values from the ZSTDConfigPatch, if any.
func (c ZSTDConfig) With(v ZSTDConfigPatch) ZSTDConfig {
	if v.Level != nil {
		c.Level = *v.Level
	}
	if v.WindowSize != nil {
		c.WindowSize = *v.WindowSize
	}
	if v.Concurrency != nil {
		c.Concurrency = *v.Concurrency
	}
	return c
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
