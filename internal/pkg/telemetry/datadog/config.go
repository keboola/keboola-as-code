package datadog

type Config struct {
	Enabled bool `configKey:"enabled" configUsage:"Enable DataDog integration."`
	Debug   bool `configKey:"debug" configUsage:"Enable DataDog debug messages."`
}

func NewConfig() Config {
	return Config{
		Enabled: true,
		Debug:   false,
	}
}
