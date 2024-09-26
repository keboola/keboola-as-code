package encoder

import "github.com/c2h5oh/datasize"

const (
	TypeCSV = Type("csv")
)

type Type string

// Config configures the local writer.
type Config struct {
	Type         Type              `json:"type" configKey:"type" configUsage:"Encoder type." validate:"required,oneof=csv"`
	Concurrency  int               `json:"concurrency" configKey:"concurrency" configUsage:"Concurrency of the format writer for the specified file type. 0 = auto = num of CPU cores" validate:"min=0,max=256"`
	RowSizeLimit datasize.ByteSize `json:"rowSizeLimit" configKey:"rowSizeLimit" configUsage:"Set's the limit of single row to be encoded. Limit should be bigger than accepted request on source otherwise received message will never be encoded" validate:"minBytes=1kB,maxBytes=2MB"`
	// OverrideEncoderFactory overrides encoder factory.
	// A custom implementation can be useful for tests.
	OverrideEncoderFactory Factory `json:"-"`
}

type ConfigPatch struct {
	Type        *string `json:"type,omitempty"`
	Concurrency *int    `json:"concurrency,omitempty"`
}

func NewConfig() Config {
	return Config{
		Type:         TypeCSV,
		Concurrency:  0,                  // 0 = auto = CPU cores
		RowSizeLimit: 1536 * datasize.KB, // ~1.5 MB
	}
}
