package diskwriter

// Config configures the local writer.
type Config struct{}

// ConfigPatch is same as the Config, but with optional/nullable fields.
// It may be part of a Sink definition to allow modification of the default configuration.
type ConfigPatch struct{}

// NewConfig provides default configuration.
func NewConfig() Config {
	return Config{}
}
