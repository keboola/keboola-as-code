package compression

import (
	"compress/gzip"
	"github.com/c2h5oh/datasize"
	"github.com/klauspost/compress/zstd"
	"runtime"
)

const (
	DefaultGZIPLevel     = gzip.BestSpeed // 1-9
	DefaultGZIPBlockSize = 256 * datasize.KB

	DefaultZSTDLevel      = zstd.SpeedFastest // 1-4
	DefaultZSDTWindowSize = 256 * datasize.KB
)

// Config for compression writer and reader.
type Config struct {
	Type Type        `json:"type" mapstructure:"type" validate:"required,oneof=none gzip zstd"  usage:"Default GZIP compression type."`
	GZIP *GZIPConfig `json:"gzip,omitempty" mapstructure:"gzip" validate:"excluded_unless=Type gzip,required_if=Type gzip"`
	ZSTD *ZSTDConfig `json:"zstd,omitempty" mapstructure:"zstd" validate:"excluded_unless=Type zstd,required_if=Type zstd"`
}

type GZIPConfig struct {
	Level       int                `json:"level" mapstructure:"level" validate:"min=1,max=9"  usage:"Default GZIP compression level."`
	Impl        GZIPImplementation `json:"impl" mapstructure:"impl" validate:"required,oneof=standard fast parallel" usage:"Default GZIP implementation: standard, fast, parallel."`
	BlockSize   datasize.ByteSize  `json:"blockSize" mapstructure:"block-size" validate:"required,min=16384,max=104857600" usage:"Default GZIP parallel block size."` //16kB-100MB
	Concurrency int                `json:"concurrency" mapstructure:"concurrency" usage:"Default GZIP parallel concurrency, 0 = auto."`
}

type ZSTDConfig struct {
	Level       zstd.EncoderLevel `json:"level" mapstructure:"level" validate:"min=1,max=4" usage:"Default ZSTD compression level."`
	WindowSize  datasize.ByteSize `json:"windowSize" mapstructure:"window-size" validate:"required,min=1024,max=536870912" usage:"Default ZSTD window size."` //1kB-512MB
	Concurrency int               `json:"concurrency" mapstructure:"concurrency" usage:"Default ZSTD concurrency, 0 = auto"`
}

func DefaultConfig() Config {
	return DefaultGZIPConfig()
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
			Level:       DefaultGZIPLevel,
			Impl:        DefaultGZIPImpl,
			BlockSize:   DefaultGZIPBlockSize,
			Concurrency: runtime.GOMAXPROCS(0),
		},
	}
}

func DefaultZSTDConfig() Config {
	return Config{
		Type: TypeZSTD,
		ZSTD: &ZSTDConfig{
			Level:       DefaultZSTDLevel,
			WindowSize:  DefaultZSDTWindowSize,
			Concurrency: runtime.GOMAXPROCS(0),
		},
	}
}
