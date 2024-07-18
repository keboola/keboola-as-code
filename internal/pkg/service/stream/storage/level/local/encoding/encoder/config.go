package encoder

const (
	TypeCSV = Type("csv")
)

type Type string

// Config configures the local writer.
type Config struct {
	Type        Type `json:"type" configKey:"type" configUsage:"Encoder type." validate:"required,oneof=csv"`
	Concurrency int  `json:"concurrency" configKey:"concurrency" configUsage:"Concurrency of the format writer for the specified file type. 0 = auto = num of CPU cores" validate:"min=0,max=256"`
	// Factory creates a high-level writer for the storage.Type, for example storage.FileTypeCSV.
	// A custom implementation can be useful for tests.
	Factory Factory `json:"-"`
}

type ConfigPatch struct {
	Type        *string `json:"type,omitempty"`
	Concurrency *int    `json:"concurrency,omitempty"`
}

func NewConfig() Config {
	return Config{
		Type:        TypeCSV,
		Concurrency: 0, // 0 = auto = CPU cores
		Factory:     DefaultFactory{},
	}
}
