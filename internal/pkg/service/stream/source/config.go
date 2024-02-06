package source

type Config struct {
	HTTP HTTPSourceConfig `configKey:"http"`
}

type HTTPSourceConfig struct {
	Listen string `configKey:"listen" configUsage:"Listen address of the HTTP source."  validate:"required,hostname_port"`
}

func NewConfig() Config {
	return Config{
		HTTP: HTTPSourceConfig{
			Listen: "0.0.0.0:7000",
		},
	}
}
