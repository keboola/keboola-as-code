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

// Config for compression writer and reader.
type Config struct {
	Type Type        `json:"type" configKey:"type" validate:"required,oneof=none gzip zstd"  configUsage:"Compression type."`
	GZIP *GZIPConfig `json:"gzip,omitempty" configKey:"gzip" validate:"required_if=Type gzip"`
	ZSTD *ZSTDConfig `json:"zstd,omitempty" configKey:"zstd" validate:"required_if=Type zstd"`
}

type GZIPConfig struct {
	Level          int                `json:"level" configKey:"level" validate:"min=1,max=9"  configUsage:"GZIP compression level: 1-9"`
	Implementation GZIPImplementation `json:"implementation" configKey:"implementation" validate:"required,oneof=standard fast parallel" configUsage:"GZIP implementation: standard, fast, parallel."`
	BlockSize      datasize.ByteSize  `json:"blockSize" configKey:"blockSize" validate:"required,min=16384,max=104857600" configUsage:"GZIP parallel block size."` // 16kB-100MB
	Concurrency    int                `json:"concurrency" configKey:"concurrency" configUsage:"GZIP parallel concurrency, 0 = auto."`
}

type ZSTDConfig struct {
	Level       zstd.EncoderLevel `json:"level" configKey:"level" validate:"min=1,max=4" configUsage:"ZSTD compression level: fastest, default, better, best"`
	WindowSize  datasize.ByteSize `json:"windowSize" configKey:"windowSize" validate:"required,min=1024,max=536870912" configUsage:"ZSTD window size."` // 1kB-512MB
	Concurrency int               `json:"concurrency" configKey:"concurrency" configUsage:"ZSTD concurrency, 0 = auto"`
}

func DefaultConfig() Config {
	return Config{
		Type: TypeGZIP,
		GZIP: DefaultGZIPConfig().GZIP,
		ZSTD: DefaultZSTDConfig().ZSTD,
	}
}

func DefaultNoneConfig() Config {
	return Config{
		Type: TypeNone,
	}
}

func DefaultGZIPConfig() Config {
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

func DefaultZSTDConfig() Config {
	return Config{
		Type: TypeZSTD,
		ZSTD: &ZSTDConfig{
			Level:       DefaultZSTDLevel,
			WindowSize:  DefaultZSDTWindowSize,
			Concurrency: 0,
		},
	}
}
