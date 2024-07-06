package encoding

// Config configures the local writer.
type Config struct {
	Concurrency int `configKey:"concurrency" configUsage:"Concurrency of the format writer for the specified file type. 0 = auto = num of CPU cores" validate:"min=0,max=256"`
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct {
	Concurrency *int `json:"concurrency,omitempty"`
}

func NewConfig() Config {
	return Config{
		Concurrency: 0, // 0 = auto = CPU cores
	}
}
