package target

// Config contains default configuration for the target storage.
type Config struct {
	// prepared for the future
}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It is part of the definition.TableSink structure to allow modification of the default configuration.
type ConfigPatch struct {
	// prepared for the future
}

// With copies values from the ConfigPatch, if any.
func (c Config) With(v ConfigPatch) Config {
	// prepared for the future
	return c
}

func NewConfig() Config {
	return Config{}
}
