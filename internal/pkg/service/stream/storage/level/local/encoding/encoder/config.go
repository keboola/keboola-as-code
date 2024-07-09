package encoder

// Config configures the local writer.
type Config struct {
	Concurrency int `configKey:"concurrency" configUsage:"Concurrency of the format writer for the specified file type. 0 = auto = num of CPU cores" validate:"min=0,max=256"`
	// Factory creates a high-level writer for the storage.FileType, for example storage.FileTypeCSV.
	// A custom implementation can be useful for tests.
	Factory Factory
}

type ConfigPatch struct {
	Concurrency *int `json:"concurrency,omitempty"`
}

func NewConfig() Config {
	return Config{
		Concurrency: 0, // 0 = auto = CPU cores
		Factory:     DefaultFactory{},
	}
}
