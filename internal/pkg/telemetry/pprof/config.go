package pprof

type Config struct {
	Enabled bool   `configKey:"enabled" configUsage:"Enable PProf HTTP server. Don't use in the production.'"`
	Listen  string `configKey:"listen" configUsage:"Listen address of the PProf HTTP server." validate:"required,hostname_port"`
}

func NewConfig() Config {
	return Config{
		Enabled: false,
		Listen:  "0.0.0.0:4000",
	}
}
